"""Pure, offline validation; no title-to-folder generation in this milestone."""
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import re
import unicodedata
from urllib.parse import parse_qs, urlsplit

ID_RE = re.compile(r"[A-Za-z0-9_-]{11}\Z")
CUSTOMER_FIELDS = (
    "url", "video_id", "title", "description", "author", "author_url",
    "duration", "resolution", "width", "height", "view_count", "like_count",
    "crawled_at_utc", "files",
)


def canonical_url(video_id):
    if not isinstance(video_id, str) or not ID_RE.fullmatch(video_id):
        raise ValueError(f"Invalid YouTube video_id: {video_id!r}")
    return f"https://www.youtube.com/watch?v={video_id}"


def video_id_from_url(value):
    # Adapted from the existing estimator, without importing its dependencies.
    try:
        parts = urlsplit(value.strip())
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
    except (ValueError, UnicodeError, AttributeError):
        return None


def folder_key(name):
    # Compare normalized names, but NEVER normalize the actual legacy folder.
    return unicodedata.normalize("NFC", name).casefold()


def safe_component(name):
    return (isinstance(name, str) and bool(name) and name not in {".", ".."}
            and not any(c in name for c in '\\/:<>"|?*')
            and not any(ord(c) < 32 for c in name)
            and not name.endswith((" ", "."))
            and not re.fullmatch(r"(?:CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³])(?:\..*)?", name, re.I))


def missing_customer_fields(metadata, *, resolution_pending=False):
    missing = []
    for key in CUSTOMER_FIELDS:
        if resolution_pending and key in {'resolution', 'width', 'height'}:
            continue
        value = metadata.get(key)
        if value is None or value == "" or (isinstance(value, str) and not value.strip()):
            missing.append(key)
        elif key in {"duration", "width", "height"}:
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not (0 < value < float("inf")):
                missing.append(key)
        elif key in {"view_count", "like_count"}:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                missing.append(key)
        elif key == "files":
            if not isinstance(value, list) or not value or not all(safe_component(x) for x in value):
                missing.append(key)
        elif key != "files" and not isinstance(value, str) and key not in {
                "duration", "width", "height", "view_count", "like_count"}:
            missing.append(key)
    return missing


def estimated_bytes(row):
    for key, multiplier in (("filesize", 1), ("filesize_approx", 1),
                            ("estimated_total_size_mb", 1_000_000)):
        text = row.get(key)
        if not text:
            continue
        try:
            value = Decimal(str(text)) * multiplier
        except InvalidOperation as exc:
            raise ValueError(f"Invalid size in {key}: {text!r}") from exc
        if not value.is_finite() or value <= 0 or value > 2**63 - 1:
            raise ValueError(f"Invalid size in {key}: {text!r}")
        return int(value.to_integral_value(rounding=ROUND_CEILING))
    return None


@dataclass(frozen=True)
class Failure:
    status: str
    code: str
    retryable: bool = False


def classify_failure(message, legacy_status="failed"):
    text = str(message).lower()
    # Specific evidence precedes broad words such as "unavailable".
    if re.search(r"\b429\b|too many requests", text):
        return Failure("rate_limited", "HTTP_429", True)
    if re.search(r"rate[ -]?limit|temporarily blocked|quota exceeded|too many videos", text):
        return Failure("rate_limited", "RATE_LIMITED", True)
    if legacy_status == "interrupted":
        return Failure("interrupted", "INTERRUPTED", True)
    if "private video" in text:
        return Failure("restricted", "PRIVATE_VIDEO")
    if re.search(r"not made this video available in your country|not available in your country|geo.?restrict", text):
        return Failure("restricted", "GEO_RESTRICTED")
    if re.search(r"age.?restrict|confirm your age", text):
        return Failure("restricted", "AGE_RESTRICTED")
    if re.search(r"sign in|log.?in required|not a bot|captcha|members.only", text):
        return Failure("restricted", "LOGIN_REQUIRED")
    if re.search(r"removed by the uploader|deleted by the uploader|has been removed|has been deleted", text):
        return Failure("permanent_failure", "VIDEO_REMOVED")
    if re.search(r"\b(?:408|500|502|503|504)\b|timed? ?out|timeout|temporar(?:ily|y) (?:unavailable|failure)|connection (?:reset|aborted|refused)|network is unreachable|remote end closed", text):
        return Failure("retry_wait", "NETWORK_TRANSIENT", True)
    if re.search(r"name resolution|failed to resolve|dns", text):
        return Failure("retry_wait", "DNS_ERROR", True)
    if re.search(r"\b(?:401|403)\b", text):
        return Failure("insufficient", "ACCESS_UNKNOWN")
    if re.search(r"no (?:video )?formats|requested format is not available", text):
        return Failure("insufficient", "NO_FORMATS")
    if re.search(r"unavailable|not available|\b(?:404|410)\b", text):
        return Failure("insufficient", "AVAILABILITY_UNKNOWN")
    if legacy_status == "rate_limited":
        return Failure("rate_limited", "RATE_LIMITED", True)
    return Failure("error", "EXTRACTION_UNKNOWN")
