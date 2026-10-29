"""Durable metadata collection, bounded retries, batch circuit breakers and resume."""
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import random
import time
import uuid

from .artifacts import json_bytes, write_new
from .database import connect_database
from .ingest import json_text, timestamp, utc_now
from .naming import assign_names, customer_artifact, materialize_catalog
from .validation import canonical_url, classify_failure, estimated_bytes, missing_customer_fields
from .youtube_adapter import FORMAT_POLICY, YouTubeAdapter

MAX_WORKERS = 4
MIN_INTERVAL, MAX_INTERVAL = 1.0, 30.0


def future_time(seconds):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec="microseconds")


def customer_metadata(video_id, result, observed_at):
    raw, summary = result["raw"], result["summary"]
    return {
        "url": canonical_url(video_id), "video_id": video_id, "title": raw.get("title"),
        "description": raw.get("description"), "author": raw.get("uploader") or raw.get("channel"),
        "author_url": raw.get("uploader_url") or raw.get("channel_url"),
        "duration": raw.get("duration"), "resolution": summary.get("resolution"),
        "width": summary.get("width"), "height": summary.get("height"),
        "view_count": raw.get("view_count"), "like_count": raw.get("like_count"),
        "crawled_at_utc": observed_at, "files": [FORMAT_POLICY["output_filename"]],
    }


@dataclass
class Collected:
    video_id: str
    attempts: list
    result: dict | None = None
    interrupted: bool = False


def collect_video(adapter, video_id, retries=2, backoff=10):
    attempts = []
    for index in range(retries + 1):
        try:
            observed = utc_now()
            result = adapter.extract(video_id)
            attempts.append({"at": observed, "outcome": "success", "code": None, "message": None, "retryable": False})
            return Collected(video_id, attempts, result)
        except InterruptedError:
            return Collected(video_id, attempts, interrupted=True)
        except Exception as exc:
            failure = classify_failure(str(exc))
            attempts.append({"at": utc_now(), "outcome": failure.status, "code": failure.code,
                             "message": str(exc), "retryable": failure.retryable})
            if failure.status == "rate_limited":
                adapter.stop()
                break
            if failure.status != "retry_wait" or index == retries:
                break
            try:
                adapter.pause(backoff * 2**index + random.uniform(0, 1))
            except InterruptedError:
                return Collected(video_id, attempts, interrupted=True)
            except Exception:
                return Collected(video_id, attempts, interrupted=True)
    return Collected(video_id, attempts)


def selected_videos(db, *, limit=None, retry_failed=False, retry_incomplete=False, all_pages=False):
    now = utc_now()
    rows = db.execute("""SELECT * FROM videos
        WHERE (? OR download_status NOT IN ('completed_local','completed'))
        AND (metadata_status!='ready' OR (? AND collected_metadata_id IS NULL))
        AND (? OR metadata_status NOT IN ('restricted','permanent_failure'))
        AND (next_retry_at IS NULL OR next_retry_at<=?)
        ORDER BY CASE WHEN metadata_status='partial' THEN 0 ELSE 1 END,video_id""", (int(all_pages), int(all_pages), int(all_pages), now)).fetchall()
    selected = []
    for row in rows:
        if row["metadata_attempt_count"] > 0 and row["metadata_status"] in {"restricted", "permanent_failure", "insufficient", "error"} and not retry_failed:
            continue
        if row["collected_metadata_id"] is not None and row["metadata_status"] in {"partial", "insufficient", "error"}:
            if not (retry_incomplete if row["metadata_status"] == "partial" else retry_failed):
                continue
        if row["metadata_attempt_count"] >= 3 and not retry_failed:
            continue
        selected.append(row["video_id"])
        if limit is not None and len(selected) >= limit:
            break
    return selected


def persist_collected(db, data_dir, run_id, outcome):
    now = utc_now()
    prior = db.execute("SELECT * FROM videos WHERE video_id=?", (outcome.video_id,)).fetchone()
    if not outcome.attempts:
        return "interrupted"
    artifact = None
    mode = outcome.result.get('metadata_mode', 'full') if outcome.result else prior['metadata_mode']
    resolution_status = prior['resolution_status']
    if outcome.result is not None:
        token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
        raw_path = Path("raw_metadata") / outcome.video_id / (token + ".json")
        customer_path = Path("metadata") / outcome.video_id / (token + ".json")
        # The immutable raw extraction is persisted before any conversion or DB projection.
        raw_hash = write_new(Path(data_dir) / raw_path, json_bytes(outcome.result["raw"]))
        if 'basic_raw' in outcome.result:
            basic_path = raw_path.with_name(token + '-basic.json')
            basic_hash = write_new(Path(data_dir) / basic_path, json_bytes(outcome.result['basic_raw']))
            outcome.result['warnings'].append(f'Basic response preserved: {basic_path.as_posix()} sha256={basic_hash}')
        customer = customer_metadata(outcome.video_id, outcome.result, outcome.attempts[-1]["at"])
        if mode == 'basic':
            previous_payload = customer_artifact(db, data_dir, prior)
            if previous_payload:
                previous = json.loads(previous_payload.decode('utf-8-sig'))
                for field in ('resolution', 'width', 'height'):
                    if customer.get(field) is None and previous.get(field) is not None:
                        customer[field] = previous[field]
        resolution_status = 'pending_vm' if not customer.get('resolution') else 'unverified_metadata'
        missing = missing_customer_fields(customer, resolution_pending=mode=='basic')
        status, code = ("partial", "METADATA_INCOMPLETE") if missing else ("ready", None)
        raw = outcome.result["raw"]
        if raw.get("is_live") or raw.get("live_status") in {"is_live", "is_upcoming", "post_live"}:
            status, code = "insufficient", "LIVE_NOT_FINAL"
        if mode != 'basic' and (not outcome.result["summary"].get("selected_format_ids") or not customer.get("height")):
            status, code = "insufficient", "NO_FORMATS"
        for warning in outcome.result.get("warnings", []):
            failure = classify_failure(warning)
            if failure.status in {"restricted", "permanent_failure", "rate_limited"}:
                status, code = failure.status, failure.code
        customer_hash = write_new(Path(data_dir) / customer_path, json_bytes(customer))
        artifact = (raw_path, raw_hash, customer_path, customer_hash, customer, missing, status, code)
    with db:
        for attempt in outcome.attempts:
            db.execute("""INSERT INTO collection_attempts
                (run_id,video_id,attempted_at_utc,outcome,error_code,error_message,retryable)
                VALUES(?,?,?,?,?,?,?)""",
                (run_id, outcome.video_id, attempt["at"], attempt["outcome"], attempt["code"],
                 attempt["message"], int(attempt["retryable"])))
        attempt_count = prior["metadata_attempt_count"] + len(outcome.attempts)
        retry_count = prior["retry_count"] + len(outcome.attempts) - (0 if prior["metadata_attempt_count"] else 1)
        if artifact:
            raw_path, raw_hash, customer_path, customer_hash, customer, missing, status, code = artifact
            summary = outcome.result["summary"]
            size = estimated_bytes(summary)
            cursor = db.execute("""INSERT INTO collected_metadata
                (video_id,run_id,observed_at_utc,raw_path,raw_sha256,customer_path,customer_sha256,
                 metadata_status,missing_fields_json,warnings_json,selected_format_ids,
                 format_policy_version,estimated_size_bytes,estimation_method,metadata_mode,resolution_status)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (outcome.video_id, run_id, outcome.attempts[-1]["at"], raw_path.as_posix(), raw_hash,
                 customer_path.as_posix(), customer_hash, status, json_text(missing),
                 json_text(outcome.result.get("warnings", [])), summary.get("selected_format_ids"),
                 FORMAT_POLICY["version"], size, summary.get("estimation_method"), mode, resolution_status))
            db.execute("""UPDATE videos SET original_title=COALESCE(?,original_title),
                metadata_status=?,collected_metadata_id=?,estimated_size_bytes=COALESCE(?,estimated_size_bytes),size_estimation_method=?,
                error_code=?,error_stage=?,error_message=?,next_retry_at=NULL,
                metadata_attempt_count=?,retry_count=?,updated_at_utc=?,metadata_mode=?,resolution_status=? WHERE video_id=?""",
                (customer.get("title") if status not in {"restricted", "permanent_failure"} else None,
                 status, cursor.lastrowid, size, summary.get("estimation_method") if size is not None else prior['size_estimation_method'], code,
                 "metadata" if code else None, "Missing fields: " + ", ".join(missing) if missing else code,
                 attempt_count, max(0, retry_count), now, mode, resolution_status, outcome.video_id))
        else:
            last = outcome.attempts[-1]
            status = "interrupted" if outcome.interrupted else last["outcome"]
            code = "INTERRUPTED" if outcome.interrupted else last["code"]
            next_retry = future_time(1800) if status == "retry_wait" else None
            db.execute("""UPDATE videos SET metadata_status=?,error_code=?,error_stage='metadata',
                error_message=?,next_retry_at=?,metadata_attempt_count=?,retry_count=?,updated_at_utc=?
                WHERE video_id=?""",
                (status, code, last["message"], next_retry, attempt_count, max(0, retry_count), now, outcome.video_id))
        if artifact:
            assign_names(db, data_dir, video_id=outcome.video_id)
        db.execute("UPDATE collection_runs SET processed_count=processed_count+1,heartbeat_at_utc=? WHERE run_id=?", (now, run_id))
    if artifact:
        materialize_catalog(db, data_dir, video_id=outcome.video_id)
    if status in {"restricted", "permanent_failure", "insufficient", "error", "retry_wait", "rate_limited", "interrupted"}:
        from .vmtask import append_live_failure
        append_live_failure(db, Path(data_dir).resolve().parent.parent, outcome.video_id, run_id)
    return status


def collect_metadata(project_root, data_dir, *, limit=None, workers=3, interval=1, timeout=30,
                     retries=2, backoff=10, retry_failed=False, retry_incomplete=False,
                     adapter=None, progress=print, all_pages=False, manual_resume=False):
    if (workers not in range(1, MAX_WORKERS + 1) or retries not in range(0, 4) or interval < 1 or timeout <= 0
            or backoff < 0 or not all(math.isfinite(x) for x in (interval, timeout, backoff))
            or limit is not None and limit < 1):
        raise ValueError("Invalid collection options: workers 1..4, retries 0..3, positive limit/timeout, interval>=1")
    data_dir = Path(data_dir)
    db = connect_database(data_dir / "controller.sqlite3")
    run_id = "c" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    adapter = adapter or YouTubeAdapter(project_root, interval, timeout)
    status, message = "completed", None
    executor = None
    futures = {}
    try:
        cooldown = db.execute("SELECT value FROM controller_settings WHERE key='metadata_cooldown_until'").fetchone()
        cooling_down = bool(cooldown and timestamp(cooldown[0]) > utc_now())
        if cooling_down and not manual_resume:
            raise ValueError(f"Metadata batch is cooling down until {cooldown[0]} UTC")
        videos = selected_videos(db, limit=limit, retry_failed=retry_failed, retry_incomplete=retry_incomplete, all_pages=all_pages)
        now = utc_now()
        with db:
            # Holding the OS writer lock establishes that any older 'running' run was interrupted.
            db.execute("UPDATE collection_runs SET status='interrupted',completed_at_utc=? WHERE status='running'", (now,))
            db.execute("DELETE FROM controller_settings WHERE key='stop_requested'")
            db.execute("DELETE FROM controller_settings WHERE key='metadata_cooldown_until'")
            db.execute("INSERT OR REPLACE INTO controller_settings VALUES('desired_workers',?)", (str(workers),))
            db.execute("INSERT OR REPLACE INTO controller_settings VALUES('desired_interval',?)", (str(interval),))
            db.execute("INSERT INTO collection_runs VALUES(?,?,?,NULL,'running',?,0,?,NULL)",
                       (run_id, now, now, len(videos), json_text({"workers": workers, "interval": interval, "timeout": timeout,
                        "retries": retries, "limit": limit, "format_policy": FORMAT_POLICY,
                        "metadata_mode": getattr(adapter, 'mode', 'test_full'), "resolution_stage": "vm_ffprobe",
                        "manual_resume": manual_resume,
                        "previous_cooldown_until": cooldown[0] if cooldown else None})))
        iterator = iter(videos)
        completed = network_failures = access_failures = 0
        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        def target_workers():
            # The monitor can change concurrency mid-run; lowering only stops new submissions.
            row = db.execute("SELECT value FROM controller_settings WHERE key='desired_workers'").fetchone()
            try:
                return min(MAX_WORKERS, max(1, int(row[0])))
            except (TypeError, ValueError):
                return workers
        applied_interval = interval
        def apply_interval():
            nonlocal applied_interval
            row = db.execute("SELECT value FROM controller_settings WHERE key='desired_interval'").fetchone()
            try:
                value = float(row[0])
            except (TypeError, ValueError):
                return
            if not math.isfinite(value):
                return
            value = min(MAX_INTERVAL, max(MIN_INTERVAL, value))
            if value != applied_interval and hasattr(adapter, 'set_interval'):
                adapter.set_interval(value)
                applied_interval = value
        def submit_one():
            video_id = next(iterator, None)
            if video_id:
                futures[executor.submit(collect_video, adapter, video_id, retries, backoff)] = video_id
        # A deliberate retry during cooldown probes one page before restoring concurrency.
        for _ in range(1 if cooling_down and manual_resume else workers):
            submit_one()
        last_heartbeat = time.monotonic()
        while futures:
            stop_requested = db.execute("SELECT value FROM controller_settings WHERE key='stop_requested'").fetchone()
            if stop_requested and status == "completed":
                status, message = "interrupted", "Stopped by local stop command"
                adapter.stop()
            apply_interval()
            done, _ = wait(futures, timeout=1, return_when=FIRST_COMPLETED)
            if time.monotonic() - last_heartbeat > 5:
                with db:
                    db.execute("UPDATE collection_runs SET heartbeat_at_utc=? WHERE run_id=?", (utc_now(), run_id))
                last_heartbeat = time.monotonic()
            for future in done:
                video_id = futures.pop(future)
                if future.cancelled():
                    continue
                outcome = future.result()
                result_status = persist_collected(db, data_dir, run_id, outcome)
                if outcome.attempts:
                    completed += 1
                    progress(f"metadata {completed}/{len(videos)} {video_id} {result_status}", flush=True)
                    last = outcome.attempts[-1]
                    network_failures = network_failures + 1 if last["outcome"] == "retry_wait" else 0
                    access_failures = access_failures + 1 if last["code"] == "LOGIN_REQUIRED" else 0
                if result_status == "rate_limited":
                    status, message = "rate_limited", "Rate limit detected; no further requests scheduled"
                    adapter.stop()
                    with db:
                        db.execute("INSERT OR REPLACE INTO controller_settings VALUES('metadata_cooldown_until',?)", (future_time(6*3600),))
                elif network_failures >= 3 or access_failures >= 3:
                    status, message = "network_blocked", "Repeated network/access failures; batch stopped without deleting URLs"
                    adapter.stop()
                    with db:
                        db.execute("INSERT OR REPLACE INTO controller_settings VALUES('metadata_cooldown_until',?)", (future_time(600),))
            if status == "completed":
                target = target_workers()
                while len(futures) < target:
                    before = len(futures)
                    submit_one()
                    if len(futures) == before:
                        break
    except KeyboardInterrupt:
        status, message = "interrupted", "Ctrl+C: progress preserved"
        adapter.stop()
    except BaseException as exc:
        status, message = "error", str(exc)
        adapter.stop()
        raise
    finally:
        if executor:
            adapter.stop() if status != "completed" else None
            # Save successful in-flight results even after Ctrl+C or a stopped batch.
            for future, video_id in list(futures.items()):
                if future.cancel():
                    continue
                try:
                    persist_collected(db, data_dir, run_id, future.result())
                except Exception as exc:
                    status, message = "error", f"Could not save {video_id}: {exc}"
            executor.shutdown(wait=True, cancel_futures=True)
        if db.execute("SELECT 1 FROM collection_runs WHERE run_id=?", (run_id,)).fetchone():
            with db:
                assign_names(db, data_dir)
                db.execute("UPDATE collection_runs SET status=?,message=?,completed_at_utc=?,heartbeat_at_utc=? WHERE run_id=?",
                           (status, message, utc_now(), utc_now(), run_id))
            materialize_catalog(db, data_dir)
        db.close()
    return {"run_id": run_id, "status": status, "message": message}
