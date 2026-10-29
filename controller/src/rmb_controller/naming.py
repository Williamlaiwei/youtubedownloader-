"""One permanent folder assignment per video; preserve every legacy basename."""
import hashlib
import json
from pathlib import Path
import re
import unicodedata
import uuid

from .artifacts import json_bytes, verified_read, write_new
from .database import connect_database
from .ingest import utc_now
from .validation import folder_key, safe_component

NAMING_VERSION = "windows-linux-v1"
MAX_UTF8 = 180
MAX_UTF16 = 100
MAX_PATH = 240


def utf16_length(text):
    return len(text.encode("utf-16-le")) // 2


def folder_budget(data_dir):
    # Reserve the longest package path, even when building only a catalog today.
    base = str(Path(data_dir).resolve() / "staging" / ("e" * 25) / "part-20" / "videos")
    limit = min(MAX_UTF16, MAX_PATH - utf16_length(base + "/metadata.json") - 1)
    if limit < 40:
        raise ValueError("Controller path is too long; use a shorter --data-dir under controller/")
    return limit


def truncate_name(text, suffix="", max_utf16=MAX_UTF16):
    result = ""
    for char in text:
        candidate = result + char + suffix
        if len(candidate.encode("utf-8")) > MAX_UTF8 or utf16_length(candidate) > max_utf16:
            break
        result += char
    result = result.rstrip(" .") or "_"
    return result + suffix


def cleaned_title(title):
    text = unicodedata.normalize("NFC", title)
    text = "".join("_" if c in '\\/:<>"|?*' or unicodedata.category(c) == "Cc" else c for c in text)
    text = re.sub(r"[ .]+$", lambda m: "_" * len(m.group()), text)
    if not text.strip():
        text = "_"
    if not safe_component(text):
        text = "_" + text
    if not safe_component(text):
        raise ValueError("Cannot safely represent title as one path component")
    return text


def choose_folder(title, video_id, occupied, max_utf16=MAX_UTF16):
    base = cleaned_title(title)
    candidate = truncate_name(base, max_utf16=max_utf16)
    if folder_key(candidate) not in occupied:
        return candidate
    suffixes = ["__" + video_id]
    digest = hashlib.sha256(video_id.encode("ascii")).hexdigest()
    suffixes += ["__" + digest[:n] for n in (12, 20, 32, 64)]
    for suffix in suffixes:
        candidate = truncate_name(base, suffix, max_utf16)
        if safe_component(candidate) and folder_key(candidate) not in occupied:
            return candidate
    raise ValueError(f"Unable to reserve a unique folder for {video_id}")


class DatabaseFolderKeys:
    """Use the unique index rather than loading every name for each new video."""
    def __init__(self, db):
        self.db = db

    def __contains__(self, key):
        return self.db.execute("SELECT 1 FROM videos WHERE folder_name_key=?", (key,)).fetchone() is not None

    def add(self, key):
        pass  # The preceding UPDATE already registered this key in SQLite.


def assign_names(db, data_dir, video_id=None):
    budget = folder_budget(data_dir)
    occupied = (DatabaseFolderKeys(db) if video_id is not None else
                {row[0] for row in db.execute("SELECT folder_name_key FROM videos WHERE folder_name_key IS NOT NULL")})
    now = utc_now()
    count = 0
    query = "SELECT video_id,original_title FROM videos WHERE folder_name IS NULL AND original_title IS NOT NULL"
    if video_id is not None:
        query += " AND video_id=?"
    for row in db.execute(query + " ORDER BY video_id", (video_id,) if video_id is not None else ()).fetchall():
        if not row["original_title"].strip():
            continue
        name = choose_folder(row["original_title"], row["video_id"], occupied, budget)
        db.execute("""UPDATE videos SET folder_name=?,folder_name_key=?,naming_version=?,
            folder_locked_at=?,updated_at_utc=? WHERE video_id=?""",
            (name, folder_key(name), NAMING_VERSION, now, now, row["video_id"]))
        occupied.add(folder_key(name))
        count += 1
    return count


def customer_artifact(db, data_dir, video):
    if video["collected_metadata_id"] is not None:
        observation = db.execute("SELECT * FROM collected_metadata WHERE collection_id=?", (video["collected_metadata_id"],)).fetchone()
        return verified_read(data_dir, observation["customer_path"], observation["customer_sha256"])
    if video["current_metadata_version"] is not None:
        observation = db.execute("""SELECT m.kind,s.snapshot_path,s.sha256 FROM metadata_versions m
            JOIN source_files s USING(source_id) WHERE m.metadata_version_id=?""",
            (video["current_metadata_version"],)).fetchone()
        if observation["kind"] == "customer_sample":
            return verified_read(data_dir, observation["snapshot_path"], observation["sha256"])
    return None


def materialize_catalog(db, data_dir, video_id=None):
    catalog = Path(data_dir) / "catalog"
    catalog.mkdir(parents=True, exist_ok=True)
    count = 0
    query = "SELECT * FROM videos WHERE folder_name IS NOT NULL"
    if video_id is not None:
        query += " AND video_id=?"
    for video in db.execute(query + " ORDER BY video_id", (video_id,) if video_id is not None else ()).fetchall():
        folder = catalog / video["folder_name"]
        if not safe_component(video["folder_name"]) or not folder.resolve().is_relative_to(catalog.resolve()):
            raise ValueError(f"Unsafe registered catalog folder: {video['video_id']}")
        if utf16_length(str(folder / "metadata.json")) > MAX_PATH:
            raise ValueError(f"Legacy folder exceeds path budget: {video['video_id']}")
        folder.mkdir(exist_ok=True)
        # Verify the actual basename; NTFS may have matched a differently cased existing name.
        marker = folder / ".controller.json"
        identity = {"video_id": video["video_id"], "folder_name": video["folder_name"]}
        if marker.exists():
            if json.loads(marker.read_text(encoding="utf-8")) != identity:
                raise ValueError(f"Catalog identity mismatch: {video['video_id']}")
        else:
            write_new(marker, json_bytes(identity))
        data = customer_artifact(db, data_dir, video)
        if data is not None:
            target = folder / "metadata.json"
            if not target.exists():
                write_new(target, data)
            elif target.read_bytes() != data:
                # This is a generated current view; immutable source/version files remain intact.
                temporary = folder / (".new-" + uuid.uuid4().hex[:8])
                write_new(temporary, data)
                temporary.replace(target)
        count += 1
    if video_id is None:
        actual = {p.name for p in catalog.iterdir() if p.is_dir()}
        expected = {row[0] for row in db.execute("SELECT folder_name FROM videos WHERE folder_name IS NOT NULL")}
        if actual != expected:
            raise ValueError("Catalog basenames differ from registered folder_name values")
    return count


def prepare_catalog(data_dir):
    db = connect_database(Path(data_dir) / "controller.sqlite3")
    try:
        with db:
            count = assign_names(db, data_dir)
        folders = materialize_catalog(db, data_dir)
        return {"new_names": count, "catalog_folders": folders}
    finally:
        db.close()
