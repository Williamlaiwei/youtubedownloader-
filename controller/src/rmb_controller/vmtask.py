"""Publish existing metadata into 20 VM batches; never collect or download."""
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import csv
import heapq
import io
import json
import os
from pathlib import Path
import statistics
import uuid
import zipfile

from .artifacts import controller_lock, json_bytes, sha256, verified_read, write_new
from .database import connect_database
from .ingest import utc_now
from .naming import assign_names, customer_artifact
from .validation import canonical_url, folder_key, missing_customer_fields, safe_component
from .youtube_adapter import FORMAT_POLICY
from .publication import (STAGE_NAME, clear_staging, compact_snapshot, discard_staging,
                          merge_distribution, merge_snapshot, remove_archived_tree, verified_archive)

BATCHES = 20
BAD_STATUSES = {"restricted", "permanent_failure", "insufficient", "error"}
RETRY_STATUSES = {"retry_wait", "rate_limited", "interrupted"}


def batch_name(number):
    return f"batch-{number:02d}"


def csv_bytes(columns, rows):
    handle = io.StringIO(newline="")
    writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        values = {}
        for key in columns:
            value = row.get(key)
            text = "" if value is None else str(value)
            # CSV presentation only: prevent titles/error text becoming Excel formulas.
            values[key] = "'" + text if text.startswith(("=", "+", "-", "@", "\t", "\r")) else text
        writer.writerow(values)
    return handle.getvalue().encode("utf-8-sig")


def jsonl_bytes(rows):
    return b"".join((json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8") for row in rows)


def allocate_parts(db):
    rows = db.execute("SELECT video_id,assigned_part,estimated_size_bytes,actual_size_bytes FROM videos").fetchall()
    sizes = [r["estimated_size_bytes"] or r["actual_size_bytes"] for r in rows if r["estimated_size_bytes"] or r["actual_size_bytes"]]
    fallback = int(statistics.median(sizes)) if sizes else 260_000_000
    loads = [[0, 0, part] for part in range(1, BATCHES + 1)]
    for row in rows:
        if row["assigned_part"] is not None:
            load = loads[row["assigned_part"] - 1]
            load[0] += row["estimated_size_bytes"] or row["actual_size_bytes"] or fallback
            load[1] += 1
    heapq.heapify(loads)
    unassigned = [r for r in rows if r["assigned_part"] is None]
    unassigned.sort(key=lambda r: (-(r["estimated_size_bytes"] or r["actual_size_bytes"] or fallback), r["video_id"]))
    for row in unassigned:
        weight, count, part = heapq.heappop(loads)
        db.execute("UPDATE videos SET assigned_part=? WHERE video_id=?", (part, row["video_id"]))
        heapq.heappush(loads, [weight + (row["estimated_size_bytes"] or row["actual_size_bytes"] or fallback), count + 1, part])
    return fallback


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def batch_rows(path):
    with Path(path).open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def validate_vmtask(root):
    root = Path(root).resolve()
    manifest = read_json(root / "manifest.json")
    ready = read_json(root / "READY.json")
    if ready != {"export_id": manifest["export_id"], "manifest_sha256": sha256((root / "manifest.json").read_bytes())}:
        raise ValueError("VMtask publication marker mismatch")
    expected_batches = {batch_name(i) for i in range(1, BATCHES + 1)}
    allowed = expected_batches | {"manifest.json", "READY.json", "README.md", "inventory.csv", "pending_metadata.csv",
                                  "metadata_warnings.jsonl", "bad_files", "records", "_history",
                                  "worker", "packages", "vm_worker_bundle.tar.gz"}
    if manifest.get('layout_version') == 2:
        allowed = expected_batches | {'manifest.json', 'READY.json', 'README.md', 'inventory.csv',
            'pending_metadata.csv', 'metadata_warnings.jsonl', 'source_manifest.json', 'source_records.zip',
            'legacy_sample_migration.json', 'legacy_sample_cleanup.jsonl', 'bad_files_current.csv',
            'bad_files_current.jsonl', 'bad_files_retryable.csv', 'bad_files_retryable.jsonl',
            'bad_files_history.jsonl', 'bad_files_live_failures.jsonl'}
    if {p.name for p in root.iterdir()} - allowed:
        raise ValueError("VMtask contains unmanaged files; refusing to modify them")
    actual_batches = {p.name for p in root.iterdir() if p.is_dir() and p.name.startswith("batch-")}
    if actual_batches != expected_batches:
        raise ValueError("VMtask must contain exactly batch-01 through batch-20")
    ids, names = set(), set()
    totals = Counter()
    for part in range(1, BATCHES + 1):
        name = batch_name(part)
        batch = root / name
        if batch.is_symlink():
            raise ValueError("Symlink batch is not allowed")
        spec = read_json(batch / "manifest.json")
        if sha256((batch / "manifest.json").read_bytes()) != manifest["batches"][name]["manifest_sha256"]:
            raise ValueError(f"Batch manifest hash mismatch: {name}")
        if spec["assigned_part"] != part or spec["export_id"] != manifest["export_id"]:
            raise ValueError(f"Batch identity mismatch: {name}")
        expected_folders = set()
        for filename, kind in (("tasks.jsonl", "ready"), ("held_tasks.jsonl", "held")):
            if sha256((batch / filename).read_bytes()) != spec[filename + "_sha256"]:
                raise ValueError(f"Task list hash mismatch: {name}/{filename}")
            rows = batch_rows(batch / filename)
            if len(rows) != spec[kind + "_count"]:
                raise ValueError(f"Task count mismatch: {name}/{filename}")
            for task in rows:
                video_id, folder = task["video_id"], task["folder_name"]
                if video_id in ids or folder_key(folder) in names:
                    raise ValueError("Duplicate ID or cross-platform folder collision")
                if not safe_component(folder) or task["assigned_part"] != part:
                    raise ValueError("Unsafe or misassigned task folder")
                if task["metadata_path"] != "videos/" + folder + "/metadata.json":
                    raise ValueError("Task folder_name differs from metadata_path")
                target = batch / task["metadata_path"]
                if target.is_symlink() or target.parent.is_symlink() or not target.resolve().is_relative_to(batch.resolve()):
                    raise ValueError("Metadata resolves outside its batch")
                payload = target.read_bytes()
                metadata = json.loads(payload.decode("utf-8-sig"))
                if sha256(payload) != task["metadata_sha256"]:
                    raise ValueError(f"Metadata hash mismatch: {video_id}")
                if metadata.get("video_id") != video_id or metadata.get("url") != canonical_url(video_id) or task["url"] != metadata["url"]:
                    raise ValueError("Metadata/task URL or video_id mismatch")
                if metadata.get("files") != [task["output_filename"]] or task["output_filename"] != "001.mkv":
                    raise ValueError("Unexpected output media filename")
                missing = missing_customer_fields(metadata, resolution_pending=task.get('metadata_mode')=='basic')
                if (not missing and not task.get("hold_reason")) != (kind == "ready") or missing != task["missing_fields"]:
                    raise ValueError("Task completeness classification mismatch")
                if sorted(p.name for p in target.parent.iterdir()) != ["metadata.json"]:
                    raise ValueError("Delivery video folders must contain only metadata.json before download")
                ids.add(video_id)
                names.add(folder_key(folder))
                expected_folders.add(folder)
                totals[kind] += 1
        actual_folders = {p.name for p in (batch / "videos").iterdir()}
        if actual_folders != expected_folders:
            raise ValueError("Actual folder basenames differ from task folder_name values")
    if totals["ready"] != manifest["ready_count"] or totals["held"] != manifest["held_count"]:
        raise ValueError("Root metadata/task totals mismatch")
    for relative, digest in manifest["record_hashes"].items():
        verified_read(root, relative, digest)
    if manifest.get('layout_version') == 2:
        if {p.name for p in root.iterdir() if p.is_dir()} != expected_batches:
            raise ValueError('VMtask must contain only the 20 permanent batch directories')
        distribution = root.parent / 'VMpackages'
        for relative, digest in manifest.get('distribution_hashes', {}).items():
            verified_read(distribution, relative, digest)
        with zipfile.ZipFile(root / 'source_records.zip') as archive:
            for source in read_json(root / 'source_manifest.json'):
                if sha256(archive.read(source['archive_member'])) != source['sha256']:
                    raise ValueError('Original source record archive hash mismatch')
    return {"export_id": manifest["export_id"], "metadata_count": len(ids), "ready_count": totals["ready"],
            "held_count": totals["held"], "batch_count": BATCHES}


def error_record(video):
    status = video["metadata_status"]
    category = {"restricted": "access_restricted", "permanent_failure": "permanent_unavailable",
                "insufficient": "availability_unconfirmed", "error": "tool_or_data_error",
                "retry_wait": "temporary_network", "rate_limited": "rate_limited",
                "interrupted": "interrupted"}.get(status, status)
    return {"video_id": video["video_id"], "url": video["url"], "assigned_part": video["assigned_part"],
            "metadata_status": status, "category": category, "permanent": status == "permanent_failure",
            "retryable": status in RETRY_STATUSES, "error_code": video["error_code"],
            "error_message": video["error_message"], "retry_count": video["retry_count"],
            "next_retry_at": video["next_retry_at"], "updated_at_utc": video["updated_at_utc"]}


def append_live_failure(db, project_root, video_id, run_id):
    """Append new unavailable/retry events without changing a published batch snapshot."""
    root = Path(project_root) / "VMtask"
    if not (root / "READY.json").is_file():
        return
    video = db.execute("SELECT * FROM videos WHERE video_id=?", (video_id,)).fetchone()
    if video["metadata_status"] not in BAD_STATUSES | RETRY_STATUSES:
        return
    event = {"recorded_at_utc": utc_now(), "run_id": run_id, **error_record(video)}
    path = root / ('bad_files/live_failures.jsonl' if (root / 'bad_files').is_dir()
                   else 'bad_files_live_failures.jsonl')
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def source_snapshots(db, data_dir, staging, record_hashes):
    result = []
    for source in db.execute("SELECT * FROM source_files ORDER BY source_id"):
        relative = Path("records/sources") / (f"{source['source_id']:06d}-" + Path(source["relative_path"]).name)
        payload = verified_read(data_dir, source["snapshot_path"], source["sha256"])
        record_hashes[relative.as_posix()] = write_new(staging / relative, payload)
        result.append({"original_path": source["relative_path"], "kind": source["kind"],
                       "sha256": source["sha256"], "copied_path": relative.as_posix()})
    return result


def build_snapshot(db, project_root, data_dir, staging, export_id):
    fallback = allocate_parts(db)
    assign_names(db, data_dir)
    videos = db.execute("SELECT * FROM videos ORDER BY video_id").fetchall()
    tasks = {p: {"ready": [], "held": []} for p in range(1, BATCHES + 1)}
    pending, warnings, inventory, sample_plan = [], [], [], []
    packaged_ids = set()
    for video in videos:
        payload = customer_artifact(db, data_dir, video)
        part = video["assigned_part"]
        entry = {"video_id": video["video_id"], "url": video["url"], "title": video["original_title"],
                 "folder_name": video["folder_name"], "batch": batch_name(part), "metadata_status": video["metadata_status"]}
        if payload is None:
            entry["package_status"] = "pending_metadata"
            pending.append({**entry, "error_code": video["error_code"], "error_message": video["error_message"]})
        else:
            metadata = json.loads(payload.decode("utf-8-sig"))
            if metadata.get("video_id") != video["video_id"] or metadata.get("url") != video["url"]:
                raise ValueError(f"Source metadata identity mismatch: {video['video_id']}")
            folder = video["folder_name"]
            if not folder or not safe_component(folder):
                raise ValueError(f"No safe registered folder for {video['video_id']}")
            missing = missing_customer_fields(metadata, resolution_pending=video['metadata_mode']=='basic')
            blocked = video["metadata_status"] in BAD_STATUSES | RETRY_STATUSES
            hold_reason = video["error_code"] if blocked else None
            kind = "held" if missing or blocked else "ready"
            relative = "videos/" + folder + "/metadata.json"
            target = staging / batch_name(part) / relative
            if not target.resolve().is_relative_to(staging.resolve()):
                raise ValueError("VMtask path escaped staging directory")
            digest = write_new(target, payload)
            task = {"schema_version": 2, "video_id": video["video_id"], "url": video["url"],
                    "folder_name": folder, "assigned_part": part, "metadata_path": relative,
                    "metadata_sha256": digest, "estimated_size_bytes": video["estimated_size_bytes"],
                    "output_filename": "001.mkv", "format_policy": FORMAT_POLICY,
                    "metadata_status": video["metadata_status"], "missing_fields": missing,
                    "metadata_mode": video['metadata_mode'], "resolution_status": video['resolution_status'],
                    "vm_update_fields": ['resolution', 'width', 'height'],
                    "hold_reason": hold_reason}
            tasks[part][kind].append(task)
            packaged_ids.add(video["video_id"])
            entry["package_status"] = kind
            if missing or blocked:
                warnings.append({**entry, "missing_fields": missing, "hold_reason": hold_reason,
                                 "metadata_path": batch_name(part) + "/" + relative})
            legacy = db.execute("""SELECT s.relative_path FROM metadata_versions m JOIN source_files s USING(source_id)
                WHERE m.video_id=? AND m.kind='customer_sample' ORDER BY m.metadata_version_id DESC LIMIT 1""",
                (video["video_id"],)).fetchone()
            if legacy:
                source = Path(project_root) / legacy["relative_path"]
                if source.exists():
                    original = source.read_bytes()
                    # Do not delete a sample if its exact JSON has not been migrated.
                    if sha256(original) != digest:
                        raise ValueError(f"Legacy sample JSON differs from packaged version: {video['video_id']}")
                    media = source.parent / "001.mkv"
                    stat = media.stat()
                    sample_plan.append({"video_id": video["video_id"], "source_folder": source.parent.relative_to(project_root).as_posix(),
                                        "metadata_sha256": digest, "media_bytes": stat.st_size, "media_mtime_ns": stat.st_mtime_ns,
                                        "destination": batch_name(part) + "/" + relative})
        inventory.append(entry)
    batch_specs = {}
    for part in range(1, BATCHES + 1):
        name = batch_name(part)
        batch = staging / name
        (batch / "videos").mkdir(parents=True, exist_ok=True)
        spec = {"schema_version": 1, "export_id": export_id, "assigned_part": part,
                "ready_count": len(tasks[part]["ready"]), "held_count": len(tasks[part]["held"]),
                "folder_root": "videos", "worker_rule": "Use folder_name exactly; never regenerate names or process held_tasks automatically."}
        for filename, kind in (("tasks.jsonl", "ready"), ("held_tasks.jsonl", "held")):
            spec[filename + "_sha256"] = write_new(batch / filename, jsonl_bytes(tasks[part][kind]))
        digest = write_new(batch / "manifest.json", json_bytes(spec))
        batch_specs[name] = {"manifest_sha256": digest, "ready_count": spec["ready_count"], "held_count": spec["held_count"]}
    bad = [error_record(v) for v in videos if v["metadata_status"] in BAD_STATUSES]
    retry = [error_record(v) for v in videos if v["metadata_status"] in RETRY_STATUSES]
    history = []
    for r in db.execute("""SELECT a.*,s.relative_path AS source_path FROM attempts a JOIN source_files s USING(source_id)
        WHERE a.stage='metadata' AND a.legacy_status!='success' ORDER BY a.attempt_id"""):
        history.append(dict(r))
    for r in db.execute("SELECT * FROM collection_attempts WHERE outcome!='success' ORDER BY id"):
        history.append({"source": "collection_attempts", **dict(r)})
    record_hashes = {}
    def record(relative, contents):
        record_hashes[relative] = write_new(staging / relative, contents)
    columns = ["video_id", "url", "title", "folder_name", "batch", "metadata_status", "package_status"]
    record("inventory.csv", csv_bytes(columns, inventory))
    record("pending_metadata.csv", csv_bytes(columns + ["error_code", "error_message"], pending))
    record("metadata_warnings.jsonl", jsonl_bytes(warnings))
    error_columns = ["video_id", "url", "assigned_part", "metadata_status", "category", "permanent", "retryable", "error_code", "error_message", "retry_count", "next_retry_at", "updated_at_utc"]
    record("bad_files/current.csv", csv_bytes(error_columns, bad))
    record("bad_files/current.jsonl", jsonl_bytes(bad))
    record("bad_files/retryable.csv", csv_bytes(error_columns, retry))
    record("bad_files/retryable.jsonl", jsonl_bytes(retry))
    record("bad_files/history.jsonl", jsonl_bytes(history))
    # This audit stream is append-only between publications, unlike the hashed snapshot lists.
    live_log = Path(project_root) / "VMtask/bad_files_live_failures.jsonl"
    if not live_log.exists():
        live_log = Path(project_root) / "VMtask/bad_files/live_failures.jsonl"
    write_new(staging / "bad_files/live_failures.jsonl", live_log.read_bytes() if live_log.is_file() else b"")
    sources = source_snapshots(db, data_dir, staging, record_hashes)
    record("records/source_manifest.json", json_bytes(sources))
    record("records/legacy_sample_migration.json", json_bytes(sample_plan))
    previous_cleanup = Path(project_root) / 'VMtask/legacy_sample_cleanup.jsonl'
    if not previous_cleanup.exists():
        previous_cleanup = Path(project_root) / "VMtask/records/legacy_sample_cleanup.jsonl"
    if previous_cleanup.is_file():
        write_new(staging / "records/legacy_sample_cleanup.jsonl", previous_cleanup.read_bytes())
    ready_count = sum(len(t["ready"]) for t in tasks.values())
    held_count = sum(len(t["held"]) for t in tasks.values())
    manifest = {"schema_version": 1, "export_id": export_id, "created_at_utc": utc_now(),
                "total_video_ids": len(videos), "metadata_count": len(packaged_ids), "ready_count": ready_count,
                "held_count": held_count, "pending_metadata_count": len(pending), "bad_file_count": len(bad),
                "retryable_count": len(retry), "population_complete": not pending, "collection_resumed": False,
                "unknown_size_scheduling_weight_bytes": fallback, "batches": batch_specs, "record_hashes": record_hashes}
    from .packaging import package_worker_and_batches
    record_hashes.update(package_worker_and_batches(staging))
    compact_snapshot(staging, manifest)
    readme = f"""# VMtask：固定 20 個 batch

已整理 {len(packaged_ids):,} 份 metadata；可派發 {ready_count:,} 支，暫留 {held_count:,} 支。
總 URL {len(videos):,} 支，尚有 {len(pending):,} 支沒有客戶格式 metadata。

- batch-01～batch-20 是唯一的一組任務目錄。新增資料合併到原本 batch，不搬走或重建目錄。
- tasks.jsonl 的 folder_name 與 videos/<影片標題>/metadata.json 完全一致；VM 不可重新命名。
- held_tasks.jsonl 保留必要欄位缺值或不可用的項目，不自動下載。
- metadata.json 的 files=["001.mkv"] 是 VM 預定檔名；本機不下載影片。
- 輕量採集的解析度可為 null；VM 下載最高畫質並通過 ffprobe 後更新解析度。
- inventory.csv 是完整清單，pending_metadata.csv 是待採集，metadata_warnings.jsonl 是缺欄位紀錄。
- bad_files_current.csv 是不可用紀錄；只有 permanent=true 為明確永久失效。
- bad_files_retryable.csv 記錄網路錯誤、429 與中斷；不刪除 URL。
- bad_files_history.jsonl 與 bad_files_live_failures.jsonl 保留歷史及即時失敗。
- source_manifest.json 對應 source_records.zip 內的原始 TXT、抽樣與下載紀錄。
- VM 程式和 20 份獨立上傳壓縮包放在專案的 VMpackages/，不佔用此處的資料夾。
- 舊版本與被更新的檔案壓縮保存在 controller/data/vmtask_archive/，不在此處新增歷史 batch。
- 更新期間沒有 READY.json；READY.json 與 manifest.json 一致才可派發。複製任務包請在更新完成後進行。
"""
    record('README.md', readme.encode('utf-8'))
    write_new(staging / "manifest.json", json_bytes(manifest))
    write_new(staging / "READY.json", json_bytes({"export_id": export_id, "manifest_sha256": sha256((staging / "manifest.json").read_bytes())}))
    return manifest, tasks


def consolidate_legacy(db, root, staging, archive_dir, export_id):
    """Archive even a partially moved legacy tree, then keep one live directory set."""
    allowed = {batch_name(p) for p in range(1, 21)} | {
        'bad_files', 'records', 'worker', 'packages', '_history', 'manifest.json', 'READY.json',
        'inventory.csv', 'pending_metadata.csv', 'metadata_warnings.jsonl', 'README.md', 'vm_worker_bundle.tar.gz'}
    allowed |= {p.name for p in staging.iterdir() if p.is_file()}
    if any(p.name not in allowed and not p.name.startswith('.b-') for p in root.iterdir()):
        raise ValueError('VMtask contains unmanaged files; refusing consolidation')
    # Never consume media or a changed video/folder identity as part of housekeeping.
    for path in root.glob('batch-*/videos/*/*'):
        if path.name != 'metadata.json' or not path.is_file():
            raise ValueError('Only metadata.json may be consolidated from video folders')
    recovery = archive_dir / 'legacy_consolidation.json'
    if recovery.exists():
        state = read_json(recovery)
        if state['root'] != str(root.resolve()):
            raise ValueError('Legacy consolidation journal root mismatch')
        backup = Path(state['archive'])
        if backup.resolve().parent != archive_dir.resolve():
            raise ValueError('Legacy archive escaped its directory')
        hashes = state['hashes']
    else:
        backup = archive_dir / (export_id + '-legacy-tree.zip')
        hashes = verified_archive(root, backup)
        write_new(recovery, json_bytes({'root': str(root.resolve()), 'archive': str(backup.resolve()), 'hashes': hashes}))
    cleanup, live = [], []
    with zipfile.ZipFile(backup) as saved:
        for relative, digest in hashes.items():
            payload = saved.read(relative)
            if sha256(payload) != digest:
                raise ValueError('Legacy recovery archive changed')
            if relative.endswith('legacy_sample_cleanup.jsonl'):
                cleanup.extend(payload.decode('utf-8').splitlines())
            if relative.endswith('live_failures.jsonl'):
                live.extend(payload.decode('utf-8').splitlines())
    if cleanup:
        (staging / 'legacy_sample_cleanup.jsonl').write_text('\n'.join(dict.fromkeys(cleanup))+'\n', encoding='utf-8')
    if live:
        (staging / 'bad_files_live_failures.jsonl').write_text('\n'.join(dict.fromkeys(live))+'\n', encoding='utf-8')
    (root / 'READY.json').unlink(missing_ok=True)
    desired = {p.name for p in staging.iterdir()}
    for path in list(root.iterdir()):
        if path.is_dir() and not path.name.startswith('batch-'):
            remove_archived_tree(path, root, hashes)
        elif path.is_file() and path.name not in desired:
            if hashes[path.name] != sha256(path.read_bytes()):
                raise ValueError('Legacy file changed during consolidation')
            path.unlink()
    for old in db.execute("SELECT export_id,relative_path FROM exports WHERE relative_path='VMtask' OR relative_path LIKE 'VMtask/_history/%'").fetchall():
        index = archive_dir / (old['export_id'] + '-archive-index.json')
        payload = json_bytes({'export_id': old['export_id'],
            'archive': backup.relative_to(root.parent).as_posix(),
            'original_relative_path': old['relative_path'],
            'note': 'Verified legacy tree, including interrupted directory moves; original metadata versions remain in SQLite.'})
        if index.exists():
            if index.read_bytes() != payload:
                raise ValueError('Existing archive index differs')
        else:
            write_new(index, payload)
        db.execute('UPDATE exports SET relative_path=? WHERE export_id=?',
                   (index.relative_to(root.parent).as_posix(), old['export_id']))


def export_vmtask(project_root, *, consolidate=False):
    project_root = Path(project_root).resolve()
    data_dir = project_root / "controller/data"
    root = project_root / "VMtask"
    root.mkdir(exist_ok=True)
    if root.is_symlink() or root.resolve().parent != project_root:
        raise ValueError("VMtask must be a real directory directly inside the project")
    export_id = "vm-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    # One fixed scratch location outside the project tree, rebuilt from SQLite every time.
    stage_root = data_dir / STAGE_NAME
    staging = stage_root / 'VMtask'
    archive_dir = data_dir / 'vmtask_archive'
    journal = archive_dir / 'pending_publication.json'
    with controller_lock(data_dir), closing(connect_database(data_dir / "controller.sqlite3")) as db:
        if db.execute("SELECT 1 FROM collection_runs WHERE status='running'").fetchone():
            raise ValueError("Pause metadata collection before publishing VMtask")
        clear_staging(data_dir, project_root)
        previous_id = None
        if any(root.iterdir()) and not consolidate:
            if journal.exists():
                if read_json(journal).get('root') != str(root.resolve()):
                    raise ValueError('Publication recovery journal root mismatch')
            else:
                if not (root / 'READY.json').is_file():
                    raise ValueError('VMtask has unmanaged or unfinished files; refusing to overwrite')
                validate_vmtask(root)
            previous_id = read_json(root / 'manifest.json')['export_id']
        staging.mkdir(parents=True)
        try:
            db.execute("BEGIN IMMEDIATE")
            manifest, tasks = build_snapshot(db, project_root, data_dir, staging, export_id)
            validate_vmtask(staging)
            if consolidate and any(root.iterdir()):
                consolidate_legacy(db, root, staging, archive_dir, export_id)
            archive = merge_snapshot(root, staging, archive_dir, export_id,
                before_publish=lambda: merge_distribution(stage_root / 'VMpackages', project_root / 'VMpackages'))
            validate_vmtask(root)
            if previous_id and archive:
                db.execute('UPDATE exports SET relative_path=? WHERE export_id=?',
                           (archive.relative_to(project_root).as_posix(), previous_id))
            digest = sha256((root / "manifest.json").read_bytes())
            db.execute("INSERT INTO exports VALUES(?,?,'ready','VMtask',?,?,?,?,NULL)",
                       (export_id, manifest["created_at_utc"], manifest["metadata_count"], manifest["pending_metadata_count"],
                        json.dumps({"held_tasks_separate": True, "population_complete": manifest["population_complete"]}), digest))
            for part, groups in tasks.items():
                for task in groups["ready"] + groups["held"]:
                    db.execute("INSERT INTO export_tasks VALUES(?,?,?,?,?,?)", (export_id, task["video_id"], part,
                               task["folder_name"], json.dumps(task, ensure_ascii=False), task["metadata_sha256"]))
            db.commit()
            journal.unlink(missing_ok=True)
            (archive_dir / 'legacy_consolidation.json').unlink(missing_ok=True)
        except BaseException:
            db.rollback()
            raise
        finally:
            # The journal alone marks an interrupted publication; the next export rebuilds staging.
            if stage_root.exists():
                discard_staging(stage_root, data_dir)
    return manifest


def remove_legacy_samples(project_root):
    """Only delete exact, migrated two-file sample folders explicitly requested by user."""
    project_root = Path(project_root).resolve()
    root = project_root / "VMtask"
    supplier = project_root / "SUPPLIER-v1-1028-youtube_camera_control"
    validate_vmtask(root)
    plan = read_json(root / "legacy_sample_migration.json")
    log = root / "legacy_sample_cleanup.jsonl"
    # Validate ALL targets and their migrated JSON before deleting even one byte.
    for item in plan:
        folder = project_root / item["source_folder"]
        destination = root / item["destination"]
        if (folder.parent.resolve() != supplier.resolve() or folder.is_symlink()
                or not destination.resolve().is_relative_to(root.resolve())):
            raise ValueError("Sample cleanup target escaped the explicitly named supplier directory")
        if not folder.exists():
            continue
        names = {p.name for p in folder.iterdir()}
        if names != {"metadata.json", "001.mkv"}:
            raise ValueError(f"Unexpected contents; refusing sample deletion: {folder}")
        if any(p.is_symlink() for p in folder.iterdir()):
            raise ValueError("Refusing symlink sample deletion")
        if sha256((folder / "metadata.json").read_bytes()) != item["metadata_sha256"] or sha256(destination.read_bytes()) != item["metadata_sha256"]:
            raise ValueError("Sample metadata was not migrated byte-for-byte")
        stat = (folder / "001.mkv").stat()
        if (stat.st_size, stat.st_mtime_ns) != (item["media_bytes"], item["media_mtime_ns"]):
            raise ValueError("Sample media changed since export; refusing deletion")
    removed, freed = 0, 0
    data_dir = project_root / "controller/data"
    with controller_lock(data_dir), closing(connect_database(data_dir / "controller.sqlite3")) as db:
        for item in plan:
            folder = project_root / item["source_folder"]
            if not folder.exists():
                continue
            # Non-recursive deletion of the two exact verified files only.
            (folder / "001.mkv").unlink()
            (folder / "metadata.json").unlink()
            folder.rmdir()
            with db:
                db.execute("UPDATE videos SET download_status='not_started',actual_size_bytes=NULL,updated_at_utc=? WHERE video_id=?",
                           (utc_now(), item["video_id"]))
            with log.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({**item, "removed_at_utc": utc_now(), "result": "metadata_migrated_media_deleted"}, ensure_ascii=False) + "\n")
                handle.flush()
            removed += 1
            freed += item["media_bytes"]
    if supplier.exists() and not any(supplier.iterdir()):
        supplier.rmdir()
    return {"sample_folders_removed": removed, "media_bytes_removed": freed}
