"""YouTube metadata-only storage estimation. Python 3.11+, decimal MB/GB/TB."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import re
import shutil
import signal
import statistics
import sys
import threading
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import numpy as np
import yt_dlp
from tqdm import tqdm
from yt_dlp.version import __version__ as YTDLP_VERSION

FORMAT = "bestvideo*+bestaudio/best"
BASE = Path(__file__).resolve().parent
ID_RE = re.compile(r"[A-Za-z0-9_-]{11}\Z")
SCHEMA_VERSION = 1
COLUMNS = [
    "video_id", "url", "title", "duration_seconds", "width", "height", "resolution", "fps",
    "video_codec", "audio_codec", "video_bitrate", "audio_bitrate", "filesize", "filesize_approx",
    "estimated_total_size_mb", "estimation_method", "metadata_status", "error_message",
    "selected_format_ids", "stream_details_json", "live_status", "metadata_warnings",
    "attempt_count", "collected_at_utc", "yt_dlp_version",
]
FAILED_COLUMNS = ["video_id", "url", "metadata_status", "error_message", "attempt_count", "collected_at_utc"]


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def canonical_url(video_id):
    return f"https://www.youtube.com/watch?v={video_id}"


def positive(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (ValueError, TypeError, OverflowError):
        return None


def video_id_from_url(line):
    try:
        parts = urlsplit(line.strip())
        if parts.scheme.lower() not in {"http", "https"} or parts.username or parts.password:
            return None
        if parts.port not in (None, 80, 443):
            return None
        host = (parts.hostname or "").lower()
        path = parts.path.strip("/").split("/")
        if host in {"youtu.be", "www.youtu.be"} and len(path) == 1:
            video_id = path[0]
        elif host in {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com",
                      "youtube-nocookie.com", "www.youtube-nocookie.com"}:
            if path == ["watch"]:
                values = parse_qs(parts.query).get("v", [])
                if len(values) != 1:
                    return None
                video_id = values[0]
            elif len(path) == 2 and path[0] in {"shorts", "embed", "live", "v"}:
                video_id = path[1]
            else:
                return None
        else:
            return None
        return video_id if ID_RE.fullmatch(video_id) else None
    except (ValueError, UnicodeError):
        return None


def find_input(explicit=None):
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_file():
            raise ValueError(f"找不到輸入檔案：{path}")
        return path
    folders = list(dict.fromkeys([Path.cwd().resolve(), BASE, BASE.parent]))
    # Search the project and its input folder, never recursively into output/.venv.
    candidates = sorted({p.resolve() for folder in folders for p in folder.glob("*")
                         if p.is_file() and p.suffix.lower() == ".txt"
                         and p.name.lower() not in {"requirements.txt", "sampled_urls.txt"}},
                        key=lambda p: str(p).casefold())
    if not candidates:
        raise ValueError("目前目錄、程式目錄及其父目錄都沒有 URL TXT；請使用 --input 指定。")
    preferred = [p for p in candidates if p.name.lower() == "youtube_urls.txt"]
    chosen = (preferred or candidates)[0]
    if len(candidates) > 1:
        print(f"找到 {len(candidates)} 個 TXT，選用 {chosen}；可用 --input 明確指定。")
    return chosen


def read_input(path):
    counts = Counter(total_lines=0, blank_lines=0, invalid_lines=0, valid_url_lines=0)
    ids = set()
    # utf-8-sig accepts UTF-8 with and without BOM; PowerShell 5 Out-File uses UTF-16.
    with path.open("rb") as f:
        bom = f.read(2)
    encoding = "utf-16" if bom in (b"\xff\xfe", b"\xfe\xff") else "utf-8-sig"
    with path.open(encoding=encoding) as f:
        for line in f:
            counts["total_lines"] += 1
            line = line.strip()
            if not line:
                counts["blank_lines"] += 1
                continue
            video_id = video_id_from_url(line)
            if video_id is None:
                counts["invalid_lines"] += 1
                continue
            counts["valid_url_lines"] += 1
            ids.add(video_id)
    counts["unique_video_ids"] = len(ids)
    counts["duplicate_video_lines"] = counts["valid_url_lines"] - len(ids)
    nonblank = counts["total_lines"] - counts["blank_lines"]
    return sorted(ids), {
        **counts, "nonblank_lines": nonblank,
        "valid_url_ratio": counts["valid_url_lines"] / nonblank if nonblank else None,
        "unique_valid_url_ratio": len(ids) / nonblank if nonblank else None,
        "validation_scope": "URL 結構及 11 字元影片 ID；不代表影片仍公開或可取得",
        "encoding": encoding,
    }


def sample_ids(ids, size, seed):
    population = sorted(set(ids))
    random.Random(seed).shuffle(population)
    return population[:min(size, len(population))]


def atomic_text(path, contents):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        f.write(contents)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_json(path, data):
    atomic_text(path, json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


@contextmanager
def run_lock(out):
    lock = out / ".run.lock"
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise ValueError(f"輸出目錄已鎖定：{lock}。確認沒有其他採集程序後，才可刪除此鎖檔。") from None
    try:
        with os.fdopen(fd, "w") as f:
            f.write(f"pid={os.getpid()}\nstarted={utc_now()}\n")
        yield
    finally:
        lock.unlink(missing_ok=True)


def prepare_sample(out, ids, size, seed):
    selected = sample_ids(ids, size, seed)
    fingerprint = hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest()
    manifest_path = out / "sample_manifest.json"
    sample_path = out / "sampled_urls.txt"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("seed") != seed
                or manifest.get("population_sha256") != fingerprint or manifest.get("format") != FORMAT):
            raise ValueError("既有樣本的 seed、影片 ID 母體或格式設定不同；請使用新的 --output-dir，保留既有結果。")
        old_ids = manifest["sample_ids"]
        if len(selected) < len(old_ids) or selected[:len(old_ids)] != old_ids:
            raise ValueError("不可縮小或更換既有樣本；請使用新的 --output-dir。")
        expected = "".join(canonical_url(i) + "\n" for i in old_ids)
        current = sample_path.read_text(encoding="utf-8") if sample_path.exists() else ""
        # An interruption can occur between writing the expanded list and its manifest.
        expanded = "".join(canonical_url(i) + "\n" for i in selected)
        if current not in (expected, expanded):
            raise ValueError("sampled_urls.txt 與樣本 manifest 不符；請保留檔案並檢查，避免誤用進度。")
    else:
        if any((out / name).exists() for name in ["sampled_urls.txt", "metadata_results.csv", "failed_urls.csv"]):
            raise ValueError("輸出已有結果但缺少 manifest；請使用新目錄，避免覆蓋。")
        manifest = {"schema_version": SCHEMA_VERSION, "seed": seed, "format": FORMAT,
                    "population_sha256": fingerprint, "created_at_utc": utc_now()}
    atomic_text(sample_path, "".join(canonical_url(i) + "\n" for i in selected))
    manifest.update(sample_ids=selected, requested_sample_size=size, updated_at_utc=utc_now())
    atomic_json(manifest_path, manifest)
    return selected


@contextmanager
def finish_write_before_interrupt():
    # Defer Ctrl+C until the CSV row and its fsync have completed.
    previous = signal.getsignal(signal.SIGINT)
    if previous == signal.SIG_IGN:
        yield
        return
    pending = []
    signal.signal(signal.SIGINT, lambda *_: pending.append(True))
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)
    if pending:
        raise KeyboardInterrupt


@contextmanager
def protect_cleanup():
    """Repeated Ctrl+C must not interrupt saving reports or draining stopped workers."""
    previous = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


def append_csv(path, columns, row=None):
    exists = path.exists() and path.stat().st_size > 0
    if exists:
        with path.open(encoding="utf-8-sig", newline="") as f:
            if next(csv.reader(f), []) != columns:
                raise ValueError(f"CSV 欄位不相容，拒絕附加：{path}")
    with finish_write_before_interrupt():
        with path.open("a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            if not exists:
                writer.writeheader()
            if row is not None:
                writer.writerow(row)
            f.flush()
            os.fsync(f.fileno())


def load_latest(path):
    if not path.exists():
        return {}
    latest = {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, strict=True)
        if reader.fieldnames != COLUMNS:
            raise ValueError(f"CSV 欄位不相容：{path}")
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"CSV 有不完整資料，請先保留並檢查檔案：{path}")
            if not ID_RE.fullmatch(row["video_id"]) or row["metadata_status"] not in {"success", "failed", "rate_limited", "interrupted"}:
                raise ValueError(f"CSV 資料格式不正確：{path}")
            latest[row["video_id"]] = row
    return latest


def has_video(fmt):
    return fmt.get("vcodec") not in (None, "none")


def has_audio(fmt):
    return fmt.get("acodec") not in (None, "none")


def selected_streams(info):
    """Honor actual selected streams; use yt-dlp's own selector for raw formats."""
    if info.get("requested_formats"):
        return info["requested_formats"]
    if info.get("requested_downloads"):
        selected = info["requested_downloads"]
        if len(selected) != 1:
            return []  # This selector should produce one output, never sum alternative outputs.
        return selected[0].get("requested_formats") or [selected[0]]
    if (has_video(info) or has_audio(info)) and (info.get("format_id") or not info.get("formats")):
        return [info]
    formats = [dict(f) for f in info.get("formats", []) if has_video(f) or has_audio(f)]
    if not formats:
        return []
    # No URLs are opened by sort/build/select. Avoid guessing the best format by height alone.
    opts = ydl_options(0, 30, MetadataLogger())
    with MetadataOnlyYDL(opts) as ydl:
        context = {**info, "formats": formats}
        ydl.sort_formats(context)
        choices = list(ydl._select_formats(context["formats"], ydl.build_format_selector(FORMAT)))
    if len(choices) != 1:
        return []
    return choices[0].get("requested_formats") or [choices[0]]


def component_size(fmt, duration):
    exact = positive(fmt.get("filesize"))
    approximate = positive(fmt.get("filesize_approx"))
    if exact is not None:
        return exact, "exact"
    if approximate is not None:
        return approximate, "approximate"
    seconds = positive(fmt.get("duration")) or positive(duration)
    bitrate = positive(fmt.get("tbr"))
    if bitrate is None:
        vbr, abr = positive(fmt.get("vbr")), positive(fmt.get("abr"))
        if has_video(fmt) and has_audio(fmt):
            bitrate = vbr + abr if vbr is not None and abr is not None else None
        elif has_video(fmt) and fmt.get("acodec") == "none":
            bitrate = vbr
        elif has_audio(fmt) and fmt.get("vcodec") == "none":
            bitrate = abr
    if seconds is not None and bitrate is not None:
        return bitrate * 1000 * seconds / 8, "bitrate"
    return None, "unknown"


def base_row(video_id):
    row = dict.fromkeys(COLUMNS)
    row.update(video_id=video_id, url=canonical_url(video_id), estimation_method="unknown",
               metadata_status="success", error_message="", attempt_count=1,
               collected_at_utc=utc_now(), yt_dlp_version=YTDLP_VERSION)
    return row


def failure_row(video_id, status, error, attempts=1):
    row = base_row(video_id)
    row.update(metadata_status=status, error_message=str(error), attempt_count=attempts)
    return row


def metadata_row(video_id, info):
    row = base_row(video_id)
    row.update(title=info.get("title"), duration_seconds=positive(info.get("duration")),
               live_status=info.get("live_status"))
    streams = selected_streams(info)
    visual = next((s for s in streams if has_video(s)), None)
    audio = next((s for s in streams if has_audio(s)), None)
    if visual:
        width, height = positive(visual.get("width")), positive(visual.get("height"))
        row.update(width=int(width) if width else None, height=int(height) if height else None,
                   resolution=f"{int(width)}x{int(height)}" if width and height else (f"{int(height)}p" if height else None),
                   fps=positive(visual.get("fps")), video_codec=visual.get("vcodec"),
                   video_bitrate=positive(visual.get("vbr")) or (positive(visual.get("tbr")) if not has_audio(visual) else None))
    if audio:
        row.update(audio_codec=audio.get("acodec"), audio_bitrate=positive(audio.get("abr")) or
                   (positive(audio.get("tbr")) if not has_video(audio) else None))
    details = []
    for stream in streams:
        size, method = component_size(stream, info.get("duration"))
        details.append({"format_id": stream.get("format_id"), "video_codec": stream.get("vcodec"),
                        "audio_codec": stream.get("acodec"), "filesize": positive(stream.get("filesize")),
                        "filesize_approx": positive(stream.get("filesize_approx")),
                        "tbr_kbps": positive(stream.get("tbr")), "vbr_kbps": positive(stream.get("vbr")),
                        "abr_kbps": positive(stream.get("abr")), "estimated_bytes": size, "method": method})
    row["selected_format_ids"] = "+".join(str(s.get("format_id", "?")) for s in streams)
    row["stream_details_json"] = json.dumps(details, ensure_ascii=False, allow_nan=False)
    if info.get("is_live") or info.get("live_status") in {"is_live", "is_upcoming", "post_live"}:
        row["error_message"] = "直播尚未結束、尚未開始或仍在處理，無法估算完整容量。"
    elif not visual:
        row["error_message"] = "未取得可確認的影片串流，容量為 unknown。"
    elif not details or any(d["estimated_bytes"] is None for d in details):
        row["error_message"] = "至少一個所選串流缺少容量及必要 bitrate/duration；不以部分容量代表總量。"
    else:
        methods = {d["method"] for d in details}
        method = "bitrate" if "bitrate" in methods else ("approximate" if "approximate" in methods else "exact")
        total = sum(d["estimated_bytes"] for d in details)
        row.update(estimated_total_size_mb=total / 1_000_000, estimation_method=method)
        if method == "exact":
            row["filesize"] = int(total)
        elif method == "approximate":
            row["filesize_approx"] = total
    return row


class RateLimited(Exception):
    pass


class BatchStopped(Exception):
    """Another worker stopped the batch; this URL remains eligible for resume."""


def is_rate_limited(message):
    return bool(re.search(r"\b429\b|too many requests|rate[ -]?limit|request limit|quota exceeded|"
                          r"temporarily blocked|too many videos|限流|請求過多", str(message), re.I))


def exception_text(exc):
    parts, seen = [], set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        parts.append(str(exc))
        status = getattr(exc, "status", None) or getattr(exc, "code", None)
        if status:
            parts.append(f"HTTP {status}")
        context = getattr(exc, "exc_info", None)
        exc = (context[1] if context and len(context) > 1 else None) or exc.__cause__ or exc.__context__
    return " | ".join(dict.fromkeys(parts))


def is_transient(message):
    # 403/authentication, deleted/private, and bot challenges are never retried.
    if re.search(r"\b(?:400|401|403|404|410)\b|private video|unavailable|sign in|not a bot|captcha|removed|deleted", message, re.I):
        return False
    return bool(re.search(r"\b(?:408|500|502|503|504)\b|timed? ?out|timeout|connection (?:reset|aborted|refused)|"
                          r"temporary failure|temporarily unavailable|network is unreachable|remote end closed|"
                          r"unable to download (?:webpage|api page)|name resolution|failed to resolve", message, re.I))


class MetadataLogger:
    def __init__(self, pacer=None):
        self.messages = []
        self.pacer = pacer

    def debug(self, message):
        # Even a downloader debug message must not hide an explicit 429.
        self._record(message, keep=False)

    def warning(self, message):
        self._record(message)

    def error(self, message):
        self._record(message)

    def _record(self, message, keep=True):
        clean = re.sub(r"\x1b\[[0-9;]*m", "", str(message))
        if keep and clean not in self.messages:
            self.messages.append(clean)
        if is_rate_limited(clean):
            if self.pacer:
                self.pacer.stop()
            raise RateLimited(clean)


class RequestPacer:
    def __init__(self, interval):
        self.interval = interval
        self.next_request = 0.0
        self.lock = threading.Lock()
        self.stopped = threading.Event()

    def stop(self):
        self.stopped.set()

    def pause(self, seconds):
        if self.stopped.wait(max(0, seconds)):
            raise BatchStopped("批次已停止，保留此 URL 供下次續跑。")

    def wait(self):
        # One shared gate for ALL workers: concurrency never multiplies the request rate.
        with self.lock:
            self.pause(self.next_request - time.monotonic())
            self.next_request = time.monotonic() + self.interval


class MetadataOnlyYDL(yt_dlp.YoutubeDL):
    """Guard media entry points and pace every yt-dlp HTTP request across videos."""
    def __init__(self, params=None, pacer=None):
        self.pacer = pacer
        super().__init__(params)

    def urlopen(self, request):
        if self.pacer:
            self.pacer.wait()
        try:
            response = super().urlopen(request)
            if getattr(response, "status", None) == 429:
                response.close()
                raise RateLimited("HTTP 429: Too Many Requests")
            return response
        except Exception as exc:
            if is_rate_limited(exception_text(exc)):
                if self.pacer:
                    self.pacer.stop()
                raise RateLimited(exception_text(exc)) from exc
            raise

    def dl(self, *args, **kwargs):
        raise RuntimeError("禁止媒體下載：MetadataOnlyYDL.dl")

    def process_info(self, *args, **kwargs):
        raise RuntimeError("禁止媒體下載：MetadataOnlyYDL.process_info")

    def download(self, *args, **kwargs):
        raise RuntimeError("禁止媒體下載：MetadataOnlyYDL.download")


def ydl_options(interval, timeout, logger):
    runtimes = {name: {"path": executable} for name, command in
                [("deno", "deno"), ("node", "node"), ("bun", "bun"), ("quickjs", "qjs")]
                if (executable := shutil.which(command))}
    return {
        "format": FORMAT, "format_sort": ["res", "fps"], "format_sort_force": True,
        "allow_multiple_video_streams": False, "allow_multiple_audio_streams": False,
        "skip_download": True, "simulate": True, "check_formats": False,
        "writethumbnail": False, "write_all_thumbnails": False, "writesubtitles": False,
        "writeautomaticsub": False, "writeinfojson": False, "writedescription": False,
        "writeannotations": False, "getcomments": False, "postprocessors": [],
        "noplaylist": True, "extract_flat": False, "cachedir": False,
        "quiet": True, "no_warnings": False, "logger": logger, "noprogress": True,
        "socket_timeout": timeout, "retries": 0, "extractor_retries": 0, "fragment_retries": 0,
        "file_access_retries": 0, "ignoreerrors": False, "ignore_no_formats_error": False,
        "sleep_interval_requests": 0,  # RequestPacer covers every urlopen, including retries.
        "proxy": "", "geo_verification_proxy": "", "geo_bypass": False,
        "cookiefile": None, "cookiesfrombrowser": None, "usenetrc": False,
        "username": None, "password": None, "js_runtimes": runtimes, "remote_components": [],
    }


def collect_one(video_id, args, pacer):
    messages = []
    for attempt in range(1, args.retries + 2):
        if pacer.stopped.is_set():
            raise BatchStopped("批次已停止。")
        logger = MetadataLogger(pacer)
        try:
            with MetadataOnlyYDL(ydl_options(args.request_interval, args.timeout, logger), pacer) as ydl:
                info = ydl.extract_info(canonical_url(video_id), download=False)
            if not isinstance(info, dict) or info.get("_type") in {"playlist", "multi_video", "url", "url_transparent"}:
                raise ValueError("未取得單一影片 metadata。")
            if info.get("id") != video_id:
                raise ValueError("回傳影片 ID 與抽樣 ID 不符。")
            row = metadata_row(video_id, info)
            row["attempt_count"] = attempt
            row["metadata_warnings"] = " | ".join(dict.fromkeys(messages + logger.messages))
            return row
        except (KeyboardInterrupt, BatchStopped):
            raise
        except Exception as exc:
            message = re.sub(r"\x1b\[[0-9;]*m", "", exception_text(exc))
            messages.extend(logger.messages)
            limited = isinstance(exc, RateLimited) or is_rate_limited(message)
            if limited:
                pacer.stop()
            elif pacer.stopped.is_set():
                raise BatchStopped("其他工作已停止整批，保留此 URL 供下次續跑。") from exc
            if limited or attempt > args.retries or not is_transient(message):
                row = failure_row(video_id, "rate_limited" if limited else "failed", message, attempt)
                row["metadata_warnings"] = " | ".join(dict.fromkeys(messages))
                return row
            wait = max(args.request_interval, args.retry_backoff * 2 ** (attempt - 1))
            tqdm.write(f"暫時性錯誤，{wait:g} 秒後重試（{attempt}/{args.retries}）：{video_id}")
            pacer.pause(wait)
    raise AssertionError("unreachable")


def collect_parallel(video_ids, args, pacer, on_result, on_interrupted, on_stopping=None):
    """Workers only extract; the calling/main thread durably writes every completed row."""
    pool = ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="metadata")
    remaining = iter(video_ids)
    active = {}
    status = "completed"

    def fill_slots():
        while len(active) < args.workers and not pacer.stopped.is_set():
            video_id = next(remaining, None)
            if video_id is None:
                break
            active[pool.submit(collect_one, video_id, args, pacer)] = video_id

    def accept(row):
        nonlocal status
        if row["metadata_status"] == "rate_limited":
            status = "rate_limited"
            pacer.stop()
        on_result(row)

    try:
        fill_slots()
        while active:
            done, _ = wait(active, timeout=0.25, return_when=FIRST_COMPLETED)
            for future in done:
                try:
                    row = future.result()
                except BatchStopped:
                    row = None
                del active[future]
                if row is not None:
                    accept(row)
            fill_slots()
    except KeyboardInterrupt:
        status = "interrupted"
        pacer.stop()
        with protect_cleanup():
            if on_stopping is not None:
                on_stopping(status)
        tqdm.write("已收到 Ctrl+C，停止新請求；中斷報告已更新，等待進行中的請求結束後再更新一次。")
    finally:
        # HTTP requests already in flight cannot be recalled. Do not start more requests;
        # retain any metadata they finish returning instead of discarding successful work.
        if active:
            pacer.stop()
            for future in active:
                future.cancel()
        with protect_cleanup():
            pool.shutdown(wait=True, cancel_futures=True)
            for future, video_id in active.items():
                if future.cancelled():
                    continue
                try:
                    row = future.result()
                except (BatchStopped, KeyboardInterrupt):
                    if status == "interrupted":
                        on_interrupted(video_id)
                else:
                    accept(row)
    return status


def percentile(values, p):
    if not values:
        return None
    data = sorted(values)
    position = (len(data) - 1) * p
    lower, upper = math.floor(position), math.ceil(position)
    return data[lower] + (data[upper] - data[lower]) * (position - lower)


def bootstrap_ci(values, seed, iterations):
    if len(values) < 2:
        return None
    rng = np.random.default_rng(seed)
    data = np.asarray(values, dtype=np.float64)
    means = []
    for start in range(0, iterations, 256):
        indices = rng.integers(0, len(data), size=(min(256, iterations - start), len(data)))
        means.extend(data[indices].mean(axis=1).tolist())
    return [percentile(means, 0.025), percentile(means, 0.975)]


def resolution_bucket(row):
    # Short edge treats 1080x1920 portrait video as 1080p, not 1920p.
    width, height = positive(row.get("width")), positive(row.get("height"))
    edge = min(width, height) if width and height else height
    return f"{int(edge)}p" if edge in {720, 1080, 1440, 2160} else "其他"


def build_summary(rows, input_stats, sample_count, seed, population, iterations, status, input_path):
    succeeded = [r for r in rows if r.get("metadata_status") == "success"]
    known = [r for r in succeeded if positive(r.get("estimated_total_size_mb")) is not None]
    sizes = [float(r["estimated_total_size_mb"]) for r in known]
    durations = [float(r["duration_seconds"]) for r in known if positive(r.get("duration_seconds")) is not None]
    all_durations = [float(r["duration_seconds"]) for r in succeeded if positive(r.get("duration_seconds")) is not None]
    mean = statistics.fmean(sizes) if sizes else None
    ci = bootstrap_ci(sizes, seed, iterations)
    methods = {m: sum(r.get("estimation_method", "unknown") == m for r in rows)
               for m in ["exact", "approximate", "bitrate", "unknown"]}
    resolution = {}
    for bucket in ["720p", "1080p", "1440p", "2160p", "其他"]:
        subset = [r for r in known if resolution_bucket(r) == bucket]
        resolution[bucket] = {
            "count": len(subset), "ratio": len(subset) / len(known) if known else None,
            "mean_mb": statistics.fmean(float(r["estimated_total_size_mb"]) for r in subset) if subset else None,
            "metadata_success_count": sum(resolution_bucket(r) == bucket for r in succeeded),
        }
    total_tb = mean * 1_000_000 * population / 1_000_000_000_000 if mean is not None else None
    ratio = lambda n, d: n / d if d else None
    failures = sum(r.get("metadata_status") == "failed" for r in rows)
    retryable = sum(r.get("metadata_status") in {"rate_limited", "interrupted"} for r in rows)
    return {
        "schema_version": SCHEMA_VERSION, "generated_at_utc": utc_now(), "run_status": status,
        "yt_dlp_version": YTDLP_VERSION, "python_version": sys.version.split()[0],
        "input_file": str(input_path), "input_statistics": input_stats,
        "sample_size": sample_count, "seed": seed, "format_selector": FORMAT,
        "processed_count": len(rows), "metadata_success_count": len(succeeded),
        "failed_count": failures, "rate_limited_or_interrupted_count": retryable,
        "pending_count": sample_count - len(rows) + retryable,
        "size_known_count": len(known), "size_unknown_processed_count": len(rows) - len(known),
        "metadata_success_rate": ratio(len(succeeded), sample_count),
        "metadata_success_rate_of_processed": ratio(len(succeeded), len(rows)),
        "size_estimation_rate": ratio(len(known), sample_count),
        "size_estimation_rate_of_metadata_success": ratio(len(known), len(succeeded)),
        "missing_or_uncollected_size_ratio": ratio(sample_count - len(known), sample_count),
        "estimation_method_counts": methods,
        "metadata_warning_count": sum(bool(r.get("metadata_warnings")) for r in succeeded),
        "size_statistics": {
            "mean_mb": mean, "mean_gb": mean / 1000 if mean is not None else None,
            "median_mb": statistics.median(sizes) if sizes else None,
            "p90_mb": percentile(sizes, .90), "p95_mb": percentile(sizes, .95),
            "min_mb": min(sizes) if sizes else None, "max_mb": max(sizes) if sizes else None,
            "known_sample_total_mb": sum(sizes), "known_sample_total_gb": sum(sizes) / 1000,
            "known_sample_total_tb": sum(sizes) / 1_000_000,
        },
        "duration_statistics": {
            "mean_seconds": statistics.fmean(durations) if durations else None,
            "sample_count": len(durations), "basis": "成功取得容量估計且有時長的樣本",
            "mean_seconds_all_metadata_success": statistics.fmean(all_durations) if all_durations else None,
            "all_metadata_success_duration_count": len(all_durations),
        },
        "resolution_distribution": resolution,
        "average_resolution": {
            key: statistics.fmean(vals) if (vals := [float(r[key]) for r in known if positive(r.get(key))]) else None
            for key in ["width", "height"]
        },
        "population_estimate": {"video_count": population, "total_tb": total_tb,
                                "reserve_fraction": 0.2, "with_20_percent_reserve_tb": total_tb * 1.2 if total_tb is not None else None},
        "bootstrap_95_ci": {
            "method": "percentile nonparametric bootstrap of known-size sample means",
            "iterations": iterations, "seed": seed, "sample_count": len(sizes), "mean_mb": ci,
            "population_total_tb": [v * population / 1_000_000 for v in ci] if ci else None,
            "population_with_reserve_tb": [v * population / 1_000_000 * 1.2 for v in ci] if ci else None,
            "finite_population_correction": False,
        },
        "units": {"MB": 1_000_000, "GB": 1_000_000_000, "TB": 1_000_000_000_000, "bitrate": "kbps (1000 bits/s)"},
        "limitations": [
            "URL 有效比例只驗證 URL 結構，影片是否可讀取由抽樣 metadata 結果呈現。",
            "只以成功取得容量估計的樣本外推，失敗與 unknown 沒有當作 0，也不補抽替換。",
            "95% bootstrap 信賴區間只反映抽樣誤差，不包含 filesize_approx 或 bitrate 估算誤差。",
            "不保證失敗或無法取得容量的影片與成功樣本具有相同分布；缺失可能造成選擇偏差。",
            "最高畫質是此次無帳號、無 Cookie 環境中 yt-dlp 可列出的格式；警告或存取限制可能使格式不完整。",
            "未下載任何媒體或測試串流可下載性；精確是 metadata 宣告的串流 bytes，未以下載驗證。",
            "音影片分流相加未包含合併容器、檔案系統、其他附件及未來格式改變的額外開銷。",
            "樣本少於 2 個已知容量時不產生信賴區間；小樣本與偏態長尾的 bootstrap 區間可能不穩定。",
            "預設以指定的 56,093 支外推，與清理後 ID 數分開呈現；容量預留計算為估計值乘以 1.20。",
        ],
    }


def report_markdown(s):
    def num(value, digits=3):
        return "unknown（資料不足）" if value is None else f"{value:,.{digits}f}"
    def pct(value):
        return "unknown" if value is None else f"{value:.2%}"
    def interval(values, unit):
        return f"{num(values[0])}–{num(values[1])} {unit}" if values else "unknown（至少需要 2 個已知容量樣本）"
    st, pop, ci, inp = s["size_statistics"], s["population_estimate"], s["bootstrap_95_ci"], s["input_statistics"]
    lines = ["# YouTube 儲存容量抽樣估算報告", "", f"更新時間（UTC）：{s['generated_at_utc']}", "",
             f"批次狀態：`{s['run_status']}`；yt-dlp：`{s['yt_dlp_version']}`；seed：{s['seed']}。", "",
             "## 輸入與採集進度", "", f"- 輸入：`{s['input_file']}`",
             f"- 原始行數：{inp.get('total_lines', 0):,}；空白：{inp.get('blank_lines', 0):,}；無效：{inp.get('invalid_lines', 0):,}。",
             f"- 結構有效 URL：{inp.get('valid_url_lines', 0):,}；重複影片：{inp.get('duplicate_video_lines', 0):,}；有效不重複 ID：{inp.get('unique_video_ids', 0):,}。",
             f"- 有效 URL 比例：{pct(inp.get('valid_url_ratio'))}（有效 URL 行／非空白行，包含重複）。",
             f"- 不重複有效比例：{pct(inp.get('unique_valid_url_ratio'))}（不重複有效 ID／非空白行）。",
             f"- 抽樣 {s['sample_size']:,} 支；已處理 {s['processed_count']:,}；metadata 成功 {s['metadata_success_count']:,}；一般失敗 {s['failed_count']:,}；限流／中斷 {s['rate_limited_or_interrupted_count']:,}；待處理 {s['pending_count']:,}。",
             f"- metadata 成功率：{pct(s['metadata_success_rate'])}（成功／抽樣）；{pct(s['metadata_success_rate_of_processed'])}（成功／已處理）。",
             f"- 成功取得容量：{s['size_known_count']:,} 支，佔抽樣 {pct(s['size_estimation_rate'])}，佔 metadata 成功 {pct(s['size_estimation_rate_of_metadata_success'])}。",
             f"- 容量缺失或尚未採集比例：{pct(s['missing_or_uncollected_size_ratio'])}；已處理但容量未知：{s['size_unknown_processed_count']:,} 支。",
             f"- metadata 成功但有警告：{s['metadata_warning_count']:,} 支（詳見 CSV 的 metadata_warnings）。", "",
             "## 容量與時長", "", "統計主體為成功取得容量估計的樣本；unknown 不當作零。單位採十進位 MB／GB／TB。", "",
             "| 指標 | 數值 |", "|---|---:|",
             f"| 平均容量 | {num(st['mean_mb'])} MB / {num(st['mean_gb'], 6)} GB |",
             f"| 容量中位數 | {num(st['median_mb'])} MB |", f"| P90 | {num(st['p90_mb'])} MB |",
             f"| P95 | {num(st['p95_mb'])} MB |", f"| 最小／最大 | {num(st['min_mb'])} / {num(st['max_mb'])} MB |",
             f"| 已知大小樣本總容量 | {num(st['known_sample_total_gb'])} GB |",
             f"| 平均時長 | {num(s['duration_statistics']['mean_seconds'], 1)} 秒（有時長 {s['duration_statistics']['sample_count']} 支） |",
             f"| 平均寬／高 | {num(s['average_resolution']['width'], 1)} / {num(s['average_resolution']['height'], 1)} px |", "",
             "## 解析度分布與平均容量", "", "以短邊分類直式／橫式影片；缺少寬度時使用高度。其他包含低畫質、非標準尺寸、8K 與解析度未知。", "",
             "| 解析度 | 已知容量數 | 佔已知容量樣本 | 平均 MB | metadata 成功數 |", "|---|---:|---:|---:|---:|"]
    for key, value in s["resolution_distribution"].items():
        lines.append(f"| {key} | {value['count']} | {pct(value['ratio'])} | {num(value['mean_mb'])} | {value['metadata_success_count']} |")
    lines += ["", "## 估算來源", "", "每個串流依序採 filesize、filesize_approx、bitrate × duration；混合來源依最不確定的串流分類。任一必要串流缺失即為 unknown。", "",
              "| 方法 | 已處理影片數 |", "|---|---:|"]
    labels = {"exact": "精確（metadata 宣告）", "approximate": "近似", "bitrate": "bitrate 推估", "unknown": "unknown（含採集失敗）"}
    lines += [f"| {labels[k]} | {v} |" for k, v in s["estimation_method_counts"].items()]
    lines += ["", f"## 整批 {pop['video_count']:,} 支外推", "",
              f"- 預估總容量：**{num(pop['total_tb'])} TB**。",
              f"- 加上 20% 儲存空間預留（× 1.20）：**{num(pop['with_20_percent_reserve_tb'])} TB**。",
              f"- 平均容量 95% bootstrap 信賴區間：{interval(ci['mean_mb'], 'MB')}。",
              f"- 整批容量 95% 區間：{interval(ci['population_total_tb'], 'TB')}。",
              f"- 加上 20% 預留的區間：{interval(ci['population_with_reserve_tb'], 'TB')}。",
              f"- bootstrap：{ci['iterations']:,} 次、有放回抽樣、已知容量 {ci['sample_count']} 支、seed {ci['seed']}；未做有限母體修正。", "",
              "## 推估限制", ""]
    lines += [f"- {item}" for item in s["limitations"]]
    return "\n".join(lines) + "\n"


def save_reports(out, summary):
    # Preserve old reports too; authoritative per-video CSV is always append-only.
    existing = [p for p in [out / "summary.json", out / "report.md"] if p.exists()]
    if existing:
        archive = out / "report_history" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        archive.mkdir(parents=True, exist_ok=False)
        for path in existing:
            shutil.copy2(path, archive / path.name)
    atomic_json(out / "summary.json", summary)
    atomic_text(out / "report.md", report_markdown(summary))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="只讀 YouTube metadata，抽樣估算儲存容量；不下載媒體。")
    parser.add_argument("--input", help="URL TXT；預設自動尋找 youtube_urls.txt")
    parser.add_argument("--output-dir", type=Path, default=BASE / "output")
    parser.add_argument("--sample-size", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--workers", type=int, choices=range(1, 5), default=1,
                        help="同時處理的影片數，1–4；所有工作共用 HTTP 請求間隔")
    parser.add_argument("--population-size", type=int, default=56093)
    parser.add_argument("--request-interval", type=float, default=2.0, help="所有工作共用的 HTTP 請求開始間隔秒數（至少 1）")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, choices=range(0, 4), default=2, help="暫時性錯誤額外重試次數，0–3")
    parser.add_argument("--retry-backoff", type=float, default=10.0)
    parser.add_argument("--bootstrap-iterations", type=int, default=10000)
    parser.add_argument("--retry-failed", action="store_true", help="明確重試一般失敗；成功 metadata（含容量 unknown）仍不重查")
    parser.add_argument("--report-only", action="store_true", help="只分析既有 CSV，不發送網路請求")
    parser.add_argument("--monitor", action="store_true", help="開啟可見的本機狀態監視器，逐一採集全部 URL metadata（不下載影片）")
    args = parser.parse_args(argv)
    if args.sample_size < 1 or args.population_size < 1 or args.bootstrap_iterations < 100:
        parser.error("sample-size / population-size 必須大於零；bootstrap-iterations 至少 100。")
    if (not math.isfinite(args.request_interval) or args.request_interval < 1 or
            not math.isfinite(args.timeout) or args.timeout <= 0 or
            not math.isfinite(args.retry_backoff) or args.retry_backoff < 1):
        parser.error("request-interval、timeout、retry-backoff 必須為有限數，且分別至少 1、>0、至少 1。")
    return args


def run(args):
    input_path = find_input(args.input)
    ids, input_stats = read_input(input_path)
    if not ids:
        raise ValueError("TXT 沒有有效且不重複的 YouTube 影片 ID。")
    print(f"輸入：{input_path}\n有效不重複 ID：{len(ids):,}；抽樣：{min(args.sample_size, len(ids))}；外推母體：{args.population_size:,}")
    if len(ids) != args.population_size:
        print(f"提醒：清理後 {len(ids):,} 支，與指定外推母體 {args.population_size:,} 不同；報告保留兩者。")
    if args.sample_size > len(ids):
        print("有效 ID 不足指定抽樣數，使用全部有效 ID。")
    out = args.output_dir.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    with run_lock(out):
        selected = prepare_sample(out, ids, args.sample_size, args.seed)
        results_path = out / "metadata_results.csv"
        failed_path = out / "failed_urls.csv"
        append_csv(results_path, COLUMNS)
        append_csv(failed_path, FAILED_COLUMNS)
        latest = load_latest(results_path)
        if set(latest) - set(selected):
            raise ValueError("CSV 含抽樣名單以外的影片 ID；請檢查輸出目錄。")
        status = "report_only" if args.report_only else "completed"
        current_id = None
        pacer = RequestPacer(args.request_interval)
        def complete(row):
            video_id = row["video_id"]
            old_attempts = int(latest.get(video_id, {}).get("attempt_count") or 0)
            row["attempt_count"] = old_attempts + row["attempt_count"]
            # Write failure history first; metadata_results.csv is the resume source.
            if row["metadata_status"] != "success":
                append_csv(failed_path, FAILED_COLUMNS, row)
            append_csv(results_path, COLUMNS, row)
            latest[video_id] = row
        def interrupted(video_id):
            # A deferred Ctrl+C may arrive immediately after a successful durable write.
            latest.update(load_latest(results_path))
            if latest.get(video_id, {}).get("metadata_status") != "success":
                complete(failure_row(video_id, "interrupted", "使用者 Ctrl+C 中斷；可於下次 resume。"))
        def write_report(report_status):
            saved = load_latest(results_path)
            rows = [saved[video_id] for video_id in selected if video_id in saved]
            report = build_summary(rows, input_stats, len(selected), args.seed, args.population_size,
                                   args.bootstrap_iterations, report_status, input_path)
            report["requested_sample_size"] = args.sample_size
            report["workers"] = args.workers
            report["global_request_interval_seconds"] = args.request_interval
            save_reports(out, report)
            return report
        def stopping(report_status):
            write_report(report_status)
            tqdm.write(f"已產生中斷報告：{out / 'report.md'}（統計目前已保存的結果）")
        try:
            if not args.report_only:
                with tqdm(total=len(selected), unit="支", desc="metadata", dynamic_ncols=True) as bar:
                    def progress():
                        bar.set_postfix(成功=sum(r["metadata_status"] == "success" for r in latest.values()),
                                        失敗=sum(r["metadata_status"] != "success" for r in latest.values()),
                                        已處理=len(latest), refresh=True)
                    def received(row):
                        complete(row)
                        bar.update(1)
                        progress()
                        if row["metadata_status"] != "success":
                            tqdm.write(f"{row['metadata_status']}：{row['video_id']}：{row['error_message']}")
                        if row["metadata_status"] == "rate_limited":
                            tqdm.write("偵測到限流，停止所有工作的新請求並保存結果；已送出的請求可能仍在返回。")
                    pending_ids = []
                    for video_id in selected:
                        old = latest.get(video_id)
                        if old and (old["metadata_status"] == "success" or
                                    (old["metadata_status"] == "failed" and not args.retry_failed)):
                            bar.update(1)
                            progress()
                            continue
                        pending_ids.append(video_id)
                    print(f"工作數：{args.workers}；全部工作共用請求間隔：{args.request_interval:g} 秒")
                    if args.workers > 1:
                        status = collect_parallel(pending_ids, args, pacer, received, interrupted, stopping)
                    for video_id in pending_ids if args.workers == 1 else []:
                        current_id = video_id
                        row = collect_one(video_id, args, pacer)
                        received(row)
                        current_id = None
                        if row["metadata_status"] == "rate_limited":
                            status = "rate_limited"
                            pacer.stop()
                            break
        except KeyboardInterrupt:
            status = "interrupted"
            pacer.stop()
            with protect_cleanup():
                # Reload in case Ctrl+C arrived just after a complete, durable CSV write.
                latest = load_latest(results_path)
                if current_id and latest.get(current_id, {}).get("metadata_status") != "success":
                    complete(failure_row(current_id, "interrupted", "使用者 Ctrl+C 中斷；可於下次 resume。"))
                stopping(status)
                print("\n已安全中斷並保存進度。")
        except Exception:
            status = "error"
            raise
        finally:
            with protect_cleanup():
                summary = write_report(status)
        print(f"報告：{out / 'report.md'}\nmetadata 成功 {summary['metadata_success_count']}/{len(selected)}；已知容量 {summary['size_known_count']} 支。")
        if summary["population_estimate"]["total_tb"] is not None:
            print(f"整批估計：{summary['population_estimate']['total_tb']:.3f} TB（詳見限制與信賴區間）。")
        return 2 if status == "rate_limited" else (130 if status == "interrupted" else 0)


def main(argv=None):
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
    if sys.version_info < (3, 11):
        print("需要 Python 3.11+。", file=sys.stderr)
        return 1
    try:
        args = parse_args(argv)
        if args.monitor:
            sys.path.insert(0, str(BASE.parents[1] / "controller" / "src"))
            from rmb_controller.monitor import run_monitor
            return run_monitor(BASE.parents[1], workers=args.workers, interval=args.request_interval)
        return run(args)
    except KeyboardInterrupt:
        print("\n已中斷；已完成的 CSV 結果會保留。", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"錯誤：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
