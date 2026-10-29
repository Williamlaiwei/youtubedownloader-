"""Import existing local evidence only. This module has no network/download code."""
from dataclasses import dataclass
from datetime import datetime, timezone
import csv
import hashlib
import io
import json
from pathlib import Path
import uuid

from .database import connect_database
from .validation import (canonical_url, classify_failure, estimated_bytes, folder_key,
                         missing_customer_fields, safe_component, video_id_from_url)

ESTIMATOR = "estimate/youtube_size_estimator/output/metadata_results.csv"
FAILURES = "estimate/youtube_size_estimator/output/failed_urls.csv"
DOWNLOAD_LOG = "download_record/download_log.csv"
TIMINGS = "estimate/sample20/results.json"
SAMPLE_ROOT = "SUPPLIER-v1-1028-youtube_camera_control"
URL_INPUTS = ("videos/youtube_urls.txt", "estimate/youtube_urls.txt")
SKIP_DIRS = {".venv", "venv", "__pycache__", ".git", ".agents", ".codex", ".aws"}
MEDIA_SUFFIXES = {".mkv", ".mp4", ".webm", ".avi", ".mov", ".part"}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def json_text(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def decode_text(data):
    return data.decode("utf-16" if data[:2] in {b"\xff\xfe", b"\xfe\xff"} else "utf-8-sig")


def timestamp(value):
    if not isinstance(value, str) or not value:
        raise ValueError("Missing observation timestamp")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError(f"Observation timestamp must include timezone: {value!r}")
    return result.astimezone(timezone.utc).isoformat(timespec="microseconds")


def nonnegative_int(value, default=0):
    result = int(value) if value not in (None, "") else default
    if result < 0:
        raise ValueError(f"Expected nonnegative integer: {value!r}")
    return result


def csv_rows(data, required):
    reader = csv.DictReader(io.StringIO(decode_text(data), newline=""), strict=True)
    fields = reader.fieldnames or []
    if len(set(fields)) != len(fields) or not set(required).issubset(fields):
        raise ValueError(f"CSV missing required columns or contains duplicate headers: {required}")
    result = []
    for index, row in enumerate(reader, 1):
        if None in row or any(v is None for v in row.values()):
            raise ValueError(f"Incomplete CSV record {index}; refusing to ignore it")
        result.append(row)
    return result


@dataclass
class Source:
    path: Path
    relative: str
    kind: str
    data: bytes
    sha256: str
    records: object = None


def discover_sources(root):
    sources = []
    for dirname in ("videos", "estimate", "download_record", SAMPLE_ROOT):
        base = root / dirname
        if not base.exists():
            continue
        for directory, dirs, files in base.walk():
            dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
            for name in sorted(files):
                path = directory / name
                if path.suffix.lower() in MEDIA_SUFFIXES or path.suffix.lower() == ".pyc":
                    continue
                relative = path.relative_to(root).as_posix()
                if not path.resolve().is_relative_to(root):
                    raise ValueError(f"Source resolves outside project: {relative}")
                kind = "reference"
                if relative in URL_INPUTS:
                    kind = "url_list"
                elif relative == ESTIMATOR:
                    kind = "estimator_results"
                elif relative == FAILURES:
                    kind = "failure_history"
                elif relative == DOWNLOAD_LOG:
                    kind = "download_log"
                elif relative == TIMINGS:
                    kind = "download_timings"
                elif path.name == "metadata.json" and path.parent.parent == root / SAMPLE_ROOT:
                    kind = "customer_sample"
                data = path.read_bytes()
                sources.append(Source(path, relative, kind, data, hashlib.sha256(data).hexdigest()))
    if not any(s.kind == "url_list" for s in sources):
        raise ValueError("No known URL input found; expected videos/youtube_urls.txt or estimate/youtube_urls.txt")
    return sorted(sources, key=lambda s: s.relative)


def parse_sources(sources):
    population = set()
    samples = {}
    for source in sources:
        if source.kind == "url_list":
            records, seen = [], set()
            for index, original in enumerate(decode_text(source.data).splitlines(), 1):
                video_id = video_id_from_url(original)
                outcome = ("blank" if not original.strip() else "invalid" if video_id is None
                           else "duplicate" if video_id in seen else "valid")
                if video_id:
                    seen.add(video_id)
                    population.add(video_id)
                records.append((index, original, video_id, outcome))
            source.records = records
        elif source.kind in {"estimator_results", "failure_history", "download_log"}:
            required = {"video_id", "url"}
            if source.kind in {"estimator_results", "failure_history"}:
                required |= {"metadata_status", "error_message", "attempt_count", "collected_at_utc"}
            if source.kind == "estimator_results":
                required |= {"title", "duration_seconds", "width", "height", "resolution",
                             "estimated_total_size_mb", "estimation_method", "metadata_warnings"}
            if source.kind == "download_log":
                required |= {"title", "folder", "status", "crawled_at_utc"}
            source.records = csv_rows(source.data, required)
        elif source.kind in {"customer_sample", "download_timings"}:
            source.records = json.loads(decode_text(source.data))
    if not population:
        raise ValueError("URL inputs contain no structurally valid video IDs")
    # Validate all identities before opening a database or writing snapshots.
    for source in sources:
        if source.kind == "url_list" or source.kind == "reference":
            continue
        records = source.records if isinstance(source.records, list) else [source.records]
        for row in records:
            if not isinstance(row, dict):
                raise ValueError(f"Expected object in {source.relative}")
            video_id = video_id_from_url(row.get("url", ""))
            expected_id = row.get("video_id", video_id) if source.kind == "download_timings" else row.get("video_id")
            if video_id is None or expected_id != video_id:
                raise ValueError(f"video_id/URL mismatch in {source.relative}")
            if video_id not in population:
                raise ValueError(f"Evidence outside input population: {video_id} in {source.relative}")
            if source.kind == "customer_sample":
                if not isinstance(source.records, dict):
                    raise ValueError(f"Customer metadata must be an object: {source.relative}")
                if video_id in samples:
                    raise ValueError(f"Multiple sample folders for video_id {video_id}")
                if not safe_component(source.path.parent.name):
                    raise ValueError(f"Unsafe existing folder: {source.relative}")
                files = row.get("files")
                if (not isinstance(files, list) or not files or not all(isinstance(x, str) for x in files)
                        or len(files) != len(set(files))):
                    raise ValueError(f"Invalid or duplicate media filenames: {source.relative}")
                for name in files:
                    if not safe_component(name):
                        raise ValueError(f"Unsafe media filename in {source.relative}")
                    media = source.path.parent / name
                    if not media.resolve().is_relative_to(source.path.parent.resolve()):
                        raise ValueError(f"Media resolves outside folder: {media}")
                    if not media.is_file() or media.stat().st_size <= 0:
                        raise ValueError(f"Missing/empty existing media: {media}")
                timestamp(row.get("crawled_at_utc"))
                samples[video_id] = source
            elif source.kind in {"estimator_results", "failure_history"}:
                if row["metadata_status"] not in {"success", "failed", "rate_limited", "interrupted"}:
                    raise ValueError(f"Unknown legacy metadata status in {source.relative}")
                timestamp(row["collected_at_utc"])
                nonnegative_int(row["attempt_count"])
    for source in sources:
        if source.kind != "download_log":
            continue
        for row in source.records:
            sample = samples.get(row["video_id"])
            if sample is not None:
                metadata = sample.records
                if row["folder"] != sample.path.parent.name:
                    raise ValueError(f"Download log folder mismatch: {row['video_id']}")
                for key in ("title", "author", "author_url", "resolution", "view_count", "like_count"):
                    if key in row and key in metadata and row[key] != str(metadata[key] if metadata[key] is not None else ""):
                        raise ValueError(f"Download log/metadata mismatch: {row['video_id']} field {key}")
    return population


def register_source(db, source, data_dir, import_id):
    snapshot = data_dir / "imports" / "objects" / (source.sha256 + ".blob")
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    try:
        with snapshot.open("xb") as handle:
            handle.write(source.data)
    except FileExistsError:
        if snapshot.read_bytes() != source.data:
            raise ValueError(f"Existing source snapshot is corrupt: {snapshot}")
    db.execute("""INSERT INTO source_files
        (relative_path,sha256,size_bytes,kind,snapshot_path,first_import_id)
        VALUES(?,?,?,?,?,?) ON CONFLICT(relative_path,sha256) DO NOTHING""",
        (source.relative, source.sha256, len(source.data), source.kind,
         snapshot.relative_to(data_dir).as_posix(), import_id))
    source_id = db.execute("SELECT source_id FROM source_files WHERE relative_path=? AND sha256=?",
                           (source.relative, source.sha256)).fetchone()[0]
    db.execute("INSERT INTO run_sources VALUES(?,?)", (import_id, source_id))
    return source_id


def estimator_metadata(row):
    result = {"url": row["url"], "video_id": row["video_id"], "title": row.get("title"),
              "resolution": row.get("resolution"), "crawled_at_utc": row["collected_at_utc"]}
    for old, new in (("duration_seconds", "duration"), ("width", "width"), ("height", "height")):
        value = row.get(old)
        result[new] = float(value) if value else None
    return result


def observation(db, source_id, index, row, kind):
    if kind == "customer_sample":
        observed = timestamp(row["crawled_at_utc"])
        missing = missing_customer_fields(row)
        status, code = ("partial", "METADATA_INCOMPLETE") if missing else ("ready", None)
        imported_status = "existing_customer_json"
    else:
        observed = timestamp(row["collected_at_utc"])
        imported_status = row["metadata_status"]
        if imported_status == "success":
            missing = missing_customer_fields(estimator_metadata(row))
            status, code = "partial", "METADATA_INCOMPLETE"
            # Early legacy rows could report a placeholder as success; retain it as evidence.
            warnings = row.get("metadata_warnings", "")
            if warnings:
                failure = classify_failure(warnings)
                if failure.status in {"restricted", "permanent_failure", "rate_limited"}:
                    status, code = failure.status, failure.code
        else:
            missing = []
            failure = classify_failure(row["error_message"], imported_status)
            status, code = failure.status, failure.code
    db.execute("""INSERT INTO metadata_versions
        (video_id,source_id,record_index,kind,observed_at_utc,imported_status,
         metadata_status,error_code,missing_fields_json,payload_json)
        VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_id,record_index) DO NOTHING""",
        (row["video_id"], source_id, index, kind, observed, imported_status, status, code,
         json_text(missing), json_text(row)))
    return db.execute("SELECT * FROM metadata_versions WHERE source_id=? AND record_index=?",
                      (source_id, index)).fetchone()


def apply_observation(db, version, now):
    video = db.execute("SELECT * FROM videos WHERE video_id=?", (version["video_id"],)).fetchone()
    if video['collected_metadata_id'] is not None:
        return  # Legacy re-import must never overwrite a newer live extraction.
    if video["current_metadata_version"]:
        old = db.execute("SELECT * FROM metadata_versions WHERE metadata_version_id=?",
                         (video["current_metadata_version"],)).fetchone()
        def order(v):
            # Existing customer JSON is richer than an estimator-only summary.
            return (v["kind"] == "customer_sample", v["observed_at_utc"], v["source_id"], v["record_index"])
        if order(version) <= order(old):
            return
    payload = json.loads(version["payload_json"])
    is_sample = version["kind"] == "customer_sample"
    # A restricted extraction may return "youtube video #<id>" as a placeholder.
    # Keep it in historical payloads, never register it as the original title.
    title = payload.get("title") if is_sample or (
        version["imported_status"] == "success" and version["metadata_status"] in {"partial", "ready"}) else None
    size = None if is_sample or payload.get("metadata_status") != "success" else estimated_bytes(payload)
    missing = json.loads(version["missing_fields_json"])
    message = ("Missing customer fields: " + ", ".join(missing) if missing
               else payload.get("error_message") or None)
    db.execute("""UPDATE videos SET original_title=COALESCE(?,original_title),
        metadata_status=?, estimated_size_bytes=?, size_estimation_method=?, error_code=?,
        error_stage=?, error_message=?, retry_count=?, current_metadata_version=?, updated_at_utc=?
        WHERE video_id=?""",
        (title or None, version["metadata_status"], size,
         None if is_sample else payload.get("estimation_method"), version["error_code"],
         "metadata" if version["error_code"] else None, message,
         0 if is_sample else max(nonnegative_int(payload.get("attempt_count")) - 1, 0),
         version["metadata_version_id"], now, version["video_id"]))


def add_attempt(db, source_id, index, row, stage):
    legacy = row.get("metadata_status", row.get("status", "measured"))
    error = row.get("error_message", "")
    code = classify_failure(error, legacy).code if stage == "metadata" and legacy != "success" else None
    video_id = row.get("video_id") or video_id_from_url(row["url"])
    db.execute("""INSERT INTO attempts
        (video_id,source_id,record_index,stage,legacy_status,error_code,error_message,
         reported_attempt_count,observed_at_utc,payload_json)
        VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_id,record_index) DO NOTHING""",
        (video_id, source_id, index, stage, legacy, code, error or None,
         nonnegative_int(row.get("attempt_count")) if stage == "metadata" else None,
         row.get("collected_at_utc", row.get("crawled_at_utc")), json_text(row)))


def checked_data_dir(root, data_dir):
    result = Path(data_dir).resolve() if data_dir else root / "controller" / "data"
    boundary = (root / "controller").resolve()
    if result == boundary or not result.is_relative_to(boundary):
        raise ValueError("Controller data directory must be inside <project>/controller/")
    return result


def import_existing(project_root, data_dir=None):
    root = Path(project_root).resolve()
    data_dir = checked_data_dir(root, data_dir)
    sources = discover_sources(root)
    population = parse_sources(sources)
    import_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
    now = utc_now()
    db = connect_database(data_dir / "controller.sqlite3")
    media_evidence = []
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO import_runs VALUES(?,?,NULL,'offline_existing',?)", (import_id, now, str(root)))
        db.executemany("""INSERT INTO videos(video_id,url,created_at_utc,updated_at_utc)
            VALUES(?,?,?,?) ON CONFLICT(video_id) DO NOTHING""",
            ((video_id, canonical_url(video_id), now, now) for video_id in sorted(population)))
        for source in sources:
            source_id = register_source(db, source, data_dir, import_id)
            if source.kind == "url_list":
                db.executemany("""INSERT INTO source_lines
                    (source_id,line_number,original_text,video_id,outcome,reason)
                    VALUES(?,?,?,?,?,?) ON CONFLICT(source_id,line_number) DO NOTHING""",
                    ((source_id, i, text, vid, outcome,
                      "URL structure or video_id invalid" if outcome == "invalid" else None)
                     for i, text, vid, outcome in source.records))
            elif source.kind in {"estimator_results", "failure_history", "download_log", "download_timings"}:
                for index, row in enumerate(source.records, 1):
                    stage = "metadata" if source.kind in {"estimator_results", "failure_history"} else "download"
                    add_attempt(db, source_id, index, row, stage)
                    # failed_urls.csv is supplemental HISTORY, never current state.
                    if source.kind == "estimator_results":
                        apply_observation(db, observation(db, source_id, index, row, "estimator_summary"), now)
            elif source.kind == "customer_sample":
                row = source.records
                version = observation(db, source_id, 1, row, "customer_sample")
                apply_observation(db, version, now)
                folder = source.path.parent.name
                db.execute("""UPDATE videos SET folder_name=?,folder_name_key=?,
                    naming_version='legacy_preserved',folder_locked_at=COALESCE(folder_locked_at,?),
                    updated_at_utc=CASE WHEN folder_name IS NULL THEN ? ELSE updated_at_utc END
                    WHERE video_id=?""", (folder, folder_key(folder), now, now, row["video_id"]))
                total = 0
                for filename in row["files"]:
                    media = source.path.parent / filename
                    stat = media.stat()
                    total += stat.st_size
                    media_evidence.append((media, stat.st_size, stat.st_mtime_ns))
                    db.execute("""INSERT INTO local_media
                        (video_id,relative_path,size_bytes,mtime_ns,metadata_source_id,first_import_id)
                        VALUES(?,?,?,?,?,?) ON CONFLICT(relative_path,size_bytes,mtime_ns) DO NOTHING""",
                        (row["video_id"], media.relative_to(root).as_posix(), stat.st_size,
                         stat.st_mtime_ns, source_id, import_id))
                # Existing, non-empty files establish local completion; no QC claim is inferred.
                db.execute("""UPDATE videos SET download_status='completed_local',actual_size_bytes=?,
                    updated_at_utc=CASE WHEN download_status!='completed_local' OR actual_size_bytes IS NOT ?
                    THEN ? ELSE updated_at_utc END WHERE video_id=?""",
                    (total, total, now, row["video_id"]))
        # Abort the transaction if a source changed while the snapshot was imported.
        for source in sources:
            if hashlib.sha256(source.path.read_bytes()).hexdigest() != source.sha256:
                raise ValueError(f"Source changed during import; rerun after it stops changing: {source.relative}")
        for media, size, mtime in media_evidence:
            stat = media.stat()
            if (stat.st_size, stat.st_mtime_ns) != (size, mtime):
                raise ValueError(f"Existing media changed during import: {media}")
        db.execute("UPDATE import_runs SET completed_at_utc=? WHERE import_id=?", (utc_now(), import_id))
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()
    return import_id, data_dir
