import csv
from contextlib import closing
import json
from pathlib import Path
import socket
import sqlite3
import shutil
import sys
import unittest
import uuid
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from rmb_controller.database import connect_database
from rmb_controller.ingest import ESTIMATOR, FAILURES, SAMPLE_ROOT, import_existing
from rmb_controller.reporting import build_report, write_report
from rmb_controller.validation import classify_failure, estimated_bytes, folder_key

A, B, C, D, E = "abcdefghijk", "ABCDEFGHIJK", "123456789_-", "xyzxyzxyzxy", "99999999999"
WHEN = "2026-10-28T01:00:00+00:00"
FIELDS = ["video_id", "url", "title", "duration_seconds", "width", "height", "resolution",
          "filesize", "filesize_approx", "estimated_total_size_mb", "estimation_method",
          "metadata_status", "error_message", "metadata_warnings", "attempt_count", "collected_at_utc"]


def url(video_id):
    return "https://www.youtube.com/watch?v=" + video_id


def write_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def legacy(video_id, status, error="", **extra):
    return {"video_id": video_id, "url": url(video_id), "title": "Known title" if status == "success" else "",
            "duration_seconds": "30" if status == "success" else "", "width": "1920", "height": "1080",
            "resolution": "1920x1080", "filesize": "1234" if status == "success" else "",
            "filesize_approx": "", "estimated_total_size_mb": "", "estimation_method": "exact",
            "metadata_status": status, "error_message": error, "metadata_warnings": "",
            "attempt_count": "1", "collected_at_utc": WHEN, **extra}


def fixture(root):
    (root / "videos").mkdir()
    (root / "estimate").mkdir()
    lines = [url(x) for x in (A, B, C, D, E)]
    (root / "videos/youtube_urls.txt").write_text("\n".join(lines + [url(A) + "&t=1", "invalid", ""]) + "\n", encoding="utf-8-sig")
    (root / "estimate/youtube_urls.txt").write_text("\n".join(lines) + "\n", encoding="utf-16")
    rows = [legacy(A, "success", metadata_warnings="Private video. No formats found!"),
            legacy(A, "failed", "Private video. Corrected offline."),
            legacy(B, "interrupted", "Ctrl+C"),
            legacy(B, "success", attempt_count="2")]
    write_csv(root / ESTIMATOR, FIELDS, rows)
    failure_fields = ["video_id", "url", "metadata_status", "error_message", "attempt_count", "collected_at_utc"]
    write_csv(root / FAILURES, failure_fields,
              [{k: rows[2][k] for k in failure_fields}])
    for video_id, folder, likes in ((C, "Cafe\u0301", None), (D, "Complete", 0)):
        directory = root / SAMPLE_ROOT / folder
        directory.mkdir(parents=True)
        metadata = {"url": url(video_id), "video_id": video_id, "title": folder, "description": "Description",
                    "author": "Author", "author_url": "https://www.youtube.com/@author", "duration": 30,
                    "resolution": "1920x1080", "width": 1920, "height": 1080, "view_count": 0,
                    "like_count": likes, "crawled_at_utc": WHEN, "files": ["001.mkv"]}
        (directory / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
        # Import establishes file presence, never claims these bytes passed media QC.
        (directory / "001.mkv").write_bytes(b"fixture media bytes")
    return rows


class OfflineImportTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(__file__).resolve().parents[1] / "data" / "test_tmp"
        scratch.mkdir(parents=True, exist_ok=True)
        # Python 3.14's Windows mkdir(mode=0o700), used by tempfile, grants an
        # ACL incompatible with this managed workspace. Inherit the scratch ACL.
        self.root = scratch / ("fixture-" + uuid.uuid4().hex)
        self.root.mkdir()
        def cleanup():
            if not self.root.resolve().is_relative_to(scratch.resolve()):
                raise ValueError("Test cleanup target escaped the dedicated scratch directory")
            shutil.rmtree(self.root)
        self.addCleanup(cleanup)
        self.rows = fixture(self.root)
        self.data = self.root / "controller/data"

    def query(self, sql, parameters=()):
        with closing(connect_database(self.data / "controller.sqlite3", readonly=True)) as db:
            return [dict(row) for row in db.execute(sql, parameters)]

    def original_bytes(self):
        return {p.relative_to(self.root).as_posix(): p.read_bytes() for p in self.root.rglob("*")
                if p.is_file() and "controller" not in p.relative_to(self.root).parts}

    def test_import_is_offline_preserves_sources_and_reconciles_history(self):
        original = self.original_bytes()
        with patch.object(socket, "socket", side_effect=AssertionError("Network prohibited")):
            import_existing(self.root)
        self.assertEqual(original, self.original_bytes())
        videos = {row["video_id"]: row for row in self.query("SELECT * FROM videos")}
        self.assertEqual(len(videos), 5)  # IDs differing only by case remain distinct.
        self.assertEqual(videos[A]["error_code"], "PRIVATE_VIDEO")
        self.assertIsNone(videos[A]["original_title"])
        self.assertEqual(videos[B]["metadata_status"], "partial")
        self.assertEqual(videos[B]["retry_count"], 1)
        self.assertEqual(videos[B]["estimated_size_bytes"], 1234)
        self.assertEqual(videos[C]["folder_name"], "Cafe\u0301")
        self.assertEqual(videos[C]["folder_name_key"], folder_key("Caf\u00e9"))
        self.assertEqual(videos[C]["metadata_status"], "partial")
        self.assertEqual(videos[D]["metadata_status"], "ready")  # 0 counts are valid.
        self.assertEqual(videos[D]["download_status"], "completed_local")
        self.assertEqual(videos[E]["metadata_status"], "pending")
        self.assertTrue(all(row["assigned_part"] is None for row in videos.values()))
        report, issues = build_report(self.data)
        self.assertEqual(report["source_lines_total"], 13)
        first = next(x for x in report["url_inputs"] if x["path"].startswith("videos/"))
        self.assertEqual(first["outcomes"], {"blank": 1, "duplicate": 1, "invalid": 1, "valid": 5})
        self.assertEqual(report["estimator"]["latest_imported_status_counts"], {"failed": 1, "success": 1})
        self.assertEqual(next(x for x in issues if x["video_id"] == C)["missing_fields"], ["like_count"])

    def test_repeat_import_does_not_duplicate_evidence_or_change_video_state(self):
        import_existing(self.root)
        before = {table: self.query(f"SELECT * FROM {table}") for table in (
            "source_files", "source_lines", "metadata_versions", "attempts", "local_media", "videos")}
        import_existing(self.root)
        for table, rows in before.items():
            self.assertEqual(rows, self.query(f"SELECT * FROM {table}"), table)
        self.assertEqual(len(self.query("SELECT * FROM import_runs")), 2)
        with closing(connect_database(self.data / "controller.sqlite3", readonly=True)) as db:
            self.assertEqual(db.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_new_source_snapshot_preserves_old_correction_and_history(self):
        import_existing(self.root)
        previous = self.query("SELECT COUNT(*) AS n FROM metadata_versions")[0]["n"]
        write_csv(self.root / ESTIMATOR, FIELDS, self.rows + [legacy(A, "success", title="Recovered")])
        import_existing(self.root)
        current = self.query("SELECT * FROM videos WHERE video_id=?", (A,))[0]
        self.assertEqual(current["metadata_status"], "partial")
        self.assertEqual(current["original_title"], "Recovered")
        self.assertEqual(self.query("SELECT COUNT(*) AS n FROM metadata_versions")[0]["n"], previous + 5)
        self.assertEqual(len(self.query("SELECT * FROM source_files WHERE relative_path=?", (ESTIMATOR,))), 2)

    def test_renamed_existing_folder_aborts_without_changing_committed_state(self):
        import_existing(self.root)
        original = self.query("SELECT * FROM videos")
        (self.root / SAMPLE_ROOT / "Complete").rename(self.root / SAMPLE_ROOT / "Changed")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
            import_existing(self.root)
        self.assertEqual(self.query("SELECT * FROM videos"), original)
        self.assertEqual(len(self.query("SELECT * FROM import_runs")), 1)

    def test_unicode_folder_collision_rolls_back_whole_import(self):
        (self.root / SAMPLE_ROOT / "Complete").rename(self.root / SAMPLE_ROOT / "Caf\u00e9")
        with self.assertRaises(sqlite3.IntegrityError):
            import_existing(self.root)
        self.assertEqual(self.query("SELECT * FROM videos"), [])
        self.assertEqual(self.query("SELECT * FROM import_runs"), [])

    def test_out_of_population_evidence_rejected_before_database_creation(self):
        write_csv(self.root / ESTIMATOR, FIELDS, [legacy("00000000000", "success")])
        with self.assertRaisesRegex(ValueError, "outside input population"):
            import_existing(self.root)
        self.assertFalse((self.data / "controller.sqlite3").exists())

    def test_missing_media_never_claims_local_completion(self):
        (self.root / SAMPLE_ROOT / "Complete/001.mkv").unlink()
        with self.assertRaisesRegex(ValueError, "Missing/empty"):
            import_existing(self.root)
        self.assertFalse((self.data / "controller.sqlite3").exists())

    def test_corrupt_snapshot_is_not_overwritten(self):
        import_existing(self.root)
        source = self.query("SELECT * FROM source_files WHERE relative_path='videos/youtube_urls.txt'")[0]
        snapshot = self.data / source["snapshot_path"]
        snapshot.write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "snapshot is corrupt"):
            import_existing(self.root)
        self.assertEqual(snapshot.read_bytes(), b"corrupt")
        self.assertEqual(len(self.query("SELECT * FROM import_runs")), 1)

    def test_reports_are_new_artifacts_and_leave_database_unchanged(self):
        import_existing(self.root)
        database = self.data / "controller.sqlite3"
        original = database.read_bytes()
        first, _ = write_report(self.data)
        second, _ = write_report(self.data)
        self.assertNotEqual(first, second)
        self.assertEqual({p.name for p in first.iterdir()}, {"report.md", "report.json", "issues.jsonl"})
        self.assertEqual(database.read_bytes(), original)


class ClassificationTests(unittest.TestCase):
    def test_transient_or_ambiguous_errors_are_never_permanent(self):
        for message in ("HTTP Error 503: temporarily unavailable", "temporarily unavailable", "Connection reset", "DNS failure"):
            result = classify_failure(message)
            self.assertTrue(result.retryable, message)
            self.assertEqual(result.status, "retry_wait")
        for message in ("This video is unavailable", "HTTP 404", "HTTP 410", "HTTP 403"):
            self.assertEqual(classify_failure(message).status, "insufficient", message)
        self.assertEqual(classify_failure("Private video. No video formats found").code, "PRIVATE_VIDEO")
        self.assertEqual(classify_failure("HTTP 429 Too Many Requests").code, "HTTP_429")
        self.assertEqual(classify_failure("temporarily blocked").code, "RATE_LIMITED")
        self.assertEqual(classify_failure("Video unavailable. Removed by the uploader").status, "permanent_failure")

    def test_byte_estimates_keep_unknown_and_decimal_units(self):
        self.assertIsNone(estimated_bytes({"estimated_total_size_mb": ""}))
        self.assertEqual(estimated_bytes({"estimated_total_size_mb": "1.0000001"}), 1_000_001)
        self.assertEqual(estimated_bytes({"filesize": "10", "estimated_total_size_mb": "900"}), 10)
        with self.assertRaises(ValueError):
            estimated_bytes({"estimated_total_size_mb": "NaN"})


if __name__ == "__main__":
    unittest.main()
