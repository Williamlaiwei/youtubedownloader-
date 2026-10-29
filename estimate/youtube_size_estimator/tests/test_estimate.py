import csv
import contextlib
import io
import json
import signal
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import estimate as e


def video(**kw):
    return {"format_id": "v", "vcodec": "vp9", "acodec": "none",
            "width": 1920, "height": 1080, **kw}


def audio(**kw):
    return {"format_id": "a", "vcodec": "none", "acodec": "opus", **kw}


class SizeTests(unittest.TestCase):
    def test_separate_exact_streams_added_once(self):
        info = {"duration": 60, "filesize": 999999999,
                "requested_formats": [video(filesize=10_000_000), audio(filesize=1_000_000)]}
        row = e.metadata_row("abcdefghijk", info)
        self.assertEqual(row["estimated_total_size_mb"], 11)
        self.assertEqual(row["filesize"], 11_000_000)
        self.assertEqual(row["estimation_method"], "exact")
        self.assertEqual(row["height"], 1080)

    def test_single_muxed_stream_not_double_counted(self):
        info = video(acodec="aac", filesize=12_000_000, duration=60)
        row = e.metadata_row("abcdefghijk", info)
        self.assertEqual(row["estimated_total_size_mb"], 12)
        self.assertEqual(row["estimation_method"], "exact")

    def test_missing_audio_size_makes_total_unknown(self):
        row = e.metadata_row("abcdefghijk", {"duration": 60, "requested_formats": [
            video(filesize=10_000_000), audio()]})
        self.assertIsNone(row["estimated_total_size_mb"])
        self.assertIsNone(row["filesize"])
        self.assertEqual(row["estimation_method"], "unknown")

    def test_mixed_methods_use_least_certain_component(self):
        row = e.metadata_row("abcdefghijk", {"duration": 10, "requested_formats": [
            video(filesize=1_000_000), audio(abr=80)]})
        self.assertEqual(row["estimated_total_size_mb"], 1.1)
        self.assertIsNone(row["filesize"])
        self.assertIsNone(row["filesize_approx"])
        self.assertEqual(row["estimation_method"], "bitrate")

    def test_approx_precedes_bitrate(self):
        row = e.metadata_row("abcdefghijk", {"duration": 10, "requested_formats": [
            video(filesize=1_000_000), audio(filesize_approx=200_000, abr=80)]})
        self.assertEqual(row["estimated_total_size_mb"], 1.2)
        self.assertEqual(row["filesize_approx"], 1_200_000)
        self.assertEqual(row["estimation_method"], "approximate")

    def test_muxed_bitrate_requires_both_components_or_tbr(self):
        row = e.metadata_row("abcdefghijk", video(acodec="aac", vbr=1000, duration=10))
        self.assertIsNone(row["estimated_total_size_mb"])
        row = e.metadata_row("abcdefghijk", video(acodec="aac", tbr=1080, duration=10))
        self.assertEqual(row["estimated_total_size_mb"], 1.35)
        self.assertIsNone(row["filesize"])

    def test_missing_duration_cannot_use_bitrate(self):
        row = e.metadata_row("abcdefghijk", video(tbr=1000))
        self.assertIsNone(row["estimated_total_size_mb"])

    def test_audio_only_not_mistaken_for_video(self):
        row = e.metadata_row("abcdefghijk", audio(filesize=1000, height=2160))
        self.assertIsNone(row["height"])
        self.assertIsNone(row["estimated_total_size_mb"])

    def test_live_is_unknown_even_with_advertised_filesize(self):
        row = e.metadata_row("abcdefghijk", video(acodec="aac", filesize=1000, is_live=True))
        self.assertIsNone(row["estimated_total_size_mb"])

    def test_requested_downloads_wrapper(self):
        row = e.metadata_row("abcdefghijk", {"duration": 10, "requested_downloads": [
            {"requested_formats": [video(filesize=1000), audio(filesize=100)]}]})
        self.assertEqual(row["filesize"], 1100)

    def test_formats_fallback_uses_ytdlp_selector(self):
        info = {"duration": 60, "formats": [
            video(format_id="low", width=1280, height=720, filesize=1000, ext="mp4", url="https://example.com/v1"),
            video(format_id="high", filesize=2000, ext="mp4", url="https://example.com/v2"),
            audio(filesize=100, ext="m4a", url="https://example.com/a", height=9999)]}
        row = e.metadata_row("abcdefghijk", info)
        self.assertEqual(row["height"], 1080)
        self.assertEqual(row["filesize"], 2100)

    def test_invalid_numeric_values_not_used(self):
        row = e.metadata_row("abcdefghijk", video(filesize=float("nan"), filesize_approx=-1))
        self.assertIsNone(row["estimated_total_size_mb"])


class InputAndStateTests(unittest.TestCase):
    def test_url_validation_and_dedup(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "urls.txt"
            path.write_text("\ufeff https://www.youtube.com/watch?v=abcdefghijk&t=1 \n"
                            "https://youtu.be/abcdefghijk\nhttps://youtube.com/shorts/123456789_-\n"
                            "https://evil.youtube.com/watch?v=abcdefghijk\nnot a URL\n\n", encoding="utf-8")
            ids, stats = e.read_input(path)
            self.assertEqual(ids, ["123456789_-", "abcdefghijk"])
            self.assertEqual(stats["valid_url_lines"], 3)
            self.assertEqual(stats["duplicate_video_lines"], 1)
            self.assertEqual(stats["invalid_lines"], 2)
            self.assertEqual(stats["blank_lines"], 1)

    def test_seed_prefix_supports_expansion(self):
        ids = [f"{i:011d}" for i in range(1000)]
        self.assertEqual(e.sample_ids(ids, 3, 42), e.sample_ids(ids, 500, 42)[:3])

    def test_state_expands_and_rejects_different_seed(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            ids = [f"{i:011d}" for i in range(10)]
            first = e.prepare_sample(out, ids, 3, 42)
            expanded = e.prepare_sample(out, ids, 5, 42)
            self.assertEqual(first, expanded[:3])
            with self.assertRaises(ValueError):
                e.prepare_sample(out, ids, 5, 43)
            with self.assertRaises(ValueError):
                e.prepare_sample(out, ids, 2, 42)

    def test_append_keeps_history_and_latest_success(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "metadata_results.csv"
            e.append_csv(path, e.COLUMNS, e.failure_row("abcdefghijk", "failed", "temporary"))
            e.append_csv(path, e.COLUMNS, e.metadata_row("abcdefghijk", video(filesize=100)))
            latest = e.load_latest(path)
            self.assertEqual(latest["abcdefghijk"]["metadata_status"], "success")
            with path.open(encoding="utf-8-sig", newline="") as f:
                self.assertEqual(len(list(csv.DictReader(f))), 2)

    def test_429_and_explicit_rate_limit_detection(self):
        for message in ["HTTP Error 429: Too Many Requests", "YouTube rate limit exceeded", "This content isn't available, try again later. Your account has been rate-limited."]:
            self.assertTrue(e.is_rate_limited(message))
        self.assertFalse(e.is_rate_limited("HTTP Error 404: Not Found"))
        self.assertFalse(e.is_rate_limited("Temporary network error, please try again later"))

    def test_metadata_options_prohibit_downloads_and_hidden_retries(self):
        opts = e.ydl_options(2, 30, e.MetadataLogger())
        for key in ["skip_download", "simulate"]:
            self.assertTrue(opts[key])
        for key in ["writethumbnail", "writesubtitles", "writeautomaticsub", "check_formats"]:
            self.assertFalse(opts[key])
        self.assertEqual(opts["extractor_retries"], 0)
        self.assertEqual(opts["proxy"], "")
        self.assertFalse(opts["ignore_no_formats_error"])

    def test_rate_limited_warning_aborts_immediately(self):
        with self.assertRaises(e.RateLimited):
            e.MetadataLogger().warning("HTTP Error 429: Too Many Requests")

    def test_media_entry_points_are_blocked(self):
        with e.MetadataOnlyYDL(e.ydl_options(2, 30, e.MetadataLogger())) as ydl:
            for method in [ydl.dl, ydl.download, ydl.process_info]:
                with self.assertRaisesRegex(RuntimeError, "禁止媒體下載"):
                    method()


class CollectionTests(unittest.TestCase):
    def args(self, directory, size=3):
        root = Path(directory)
        source = root / "youtube_urls.txt"
        if not source.exists():
            source.write_text("".join(e.canonical_url(i) + "\n" for i in ["abcdefghijk", "ABCDEFGHIJK", "123456789_-"]), encoding="utf-8")
        return e.parse_args(["--input", str(source), "--output-dir", str(root / "output"),
                             "--sample-size", str(size), "--bootstrap-iterations", "100"])

    def run_quietly(self, args):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return e.run(args)

    def test_success_extracts_download_false_and_no_media(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d)
            info = video(id="abcdefghijk", acodec="aac", filesize=100)
            with patch.object(e.MetadataOnlyYDL, "extract_info", return_value=info) as extract:
                row = e.collect_one("abcdefghijk", args, e.RequestPacer(2))
            extract.assert_called_once_with(e.canonical_url("abcdefghijk"), download=False)
            self.assertEqual(row["metadata_status"], "success")

    def test_private_video_fails_without_retry(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d)
            with patch.object(e.MetadataOnlyYDL, "extract_info", side_effect=yt_error("Private video. Please sign in")) as extract:
                row = e.collect_one("abcdefghijk", args, e.RequestPacer(2))
            self.assertEqual(row["metadata_status"], "failed")
            self.assertEqual(extract.call_count, 1)

    def test_network_retry_is_limited_and_rate_limit_never_retried(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d)
            pacer = e.RequestPacer(2)
            with patch.object(e.MetadataOnlyYDL, "extract_info", side_effect=yt_error("HTTP Error 503")) as extract, patch.object(pacer, "pause") as sleep:
                row = e.collect_one("abcdefghijk", args, pacer)
            self.assertEqual(row["metadata_status"], "failed")
            self.assertEqual(extract.call_count, 3)
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [10, 20])
            with patch.object(e.MetadataOnlyYDL, "extract_info", side_effect=yt_error("HTTP Error 429")) as extract:
                row = e.collect_one("abcdefghijk", args, e.RequestPacer(2))
            self.assertEqual(row["metadata_status"], "rate_limited")
            self.assertEqual(extract.call_count, 1)

    def test_batch_stops_after_first_rate_limit_and_saves(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d)
            def limited(video_id, *_):
                return e.failure_row(video_id, "rate_limited", "HTTP 429")
            with patch.object(e, "collect_one", side_effect=limited) as collect:
                code = self.run_quietly(args)
            self.assertEqual(code, 2)
            self.assertEqual(collect.call_count, 1)
            out = args.output_dir
            summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["run_status"], "rate_limited")
            self.assertEqual(summary["pending_count"], 3)
            self.assertEqual(len(e.load_latest(out / "metadata_results.csv")), 1)
            self.assertFalse((out / ".run.lock").exists())

    def test_resume_and_expansion_skip_successful_urls(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d, 1)
            def success(video_id, *_):
                return e.metadata_row(video_id, video(filesize=100, duration=10))
            with patch.object(e, "collect_one", side_effect=success) as collect:
                self.assertEqual(self.run_quietly(args), 0)
                self.assertEqual(collect.call_count, 1)
            original = (args.output_dir / "metadata_results.csv").read_bytes()
            with patch.object(e, "collect_one", side_effect=AssertionError("must not query")):
                self.assertEqual(self.run_quietly(args), 0)
            self.assertEqual(original, (args.output_dir / "metadata_results.csv").read_bytes())
            args.sample_size = 3
            with patch.object(e, "collect_one", side_effect=success) as collect:
                self.assertEqual(self.run_quietly(args), 0)
                self.assertEqual(collect.call_count, 2)
            self.assertTrue((args.output_dir / "metadata_results.csv").read_bytes().startswith(original))

    def test_ctrl_c_saves_current_url_then_resume(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d)
            with patch.object(e, "collect_one", side_effect=KeyboardInterrupt):
                self.assertEqual(self.run_quietly(args), 130)
            summary = json.loads((args.output_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["run_status"], "interrupted")
            self.assertEqual(summary["pending_count"], 3)
            self.assertTrue((args.output_dir / "report.md").exists())
            self.assertIn("`interrupted`", (args.output_dir / "report.md").read_text(encoding="utf-8"))
            self.assertFalse((args.output_dir / ".run.lock").exists())
            with patch.object(e, "collect_one", side_effect=lambda video_id, *_: e.metadata_row(video_id, video(filesize=100))) as collect:
                self.assertEqual(self.run_quietly(args), 0)
            self.assertEqual(collect.call_count, 3)

    def test_report_only_makes_no_network_requests(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d)
            args.report_only = True
            with patch.object(e.MetadataOnlyYDL, "urlopen", side_effect=AssertionError("no network")), patch.object(e, "collect_one", side_effect=AssertionError("no collection")):
                self.assertEqual(self.run_quietly(args), 0)


class ParallelTests(unittest.TestCase):
    args = CollectionTests.args
    run_quietly = CollectionTests.run_quietly

    def test_three_workers_overlap_and_only_main_thread_writes(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d)
            args.workers = 3
            barrier = threading.Barrier(3)
            worker_threads, writer_threads = set(), set()
            append = e.append_csv

            def extract(video_id, *_):
                worker_threads.add(threading.get_ident())
                barrier.wait(timeout=5)
                return e.metadata_row(video_id, video(filesize=100))

            def write(*args, **kwargs):
                writer_threads.add(threading.get_ident())
                return append(*args, **kwargs)

            with patch.object(e, "collect_one", side_effect=extract), patch.object(e, "append_csv", side_effect=write):
                self.assertEqual(self.run_quietly(args), 0)
            self.assertEqual(len(worker_threads), 3)
            self.assertEqual(writer_threads, {threading.get_ident()})
            self.assertEqual(len(e.load_latest(args.output_dir / "metadata_results.csv")), 3)
            before = (args.output_dir / "metadata_results.csv").read_bytes()
            with patch.object(e, "collect_one", side_effect=AssertionError("must not query")):
                self.assertEqual(self.run_quietly(args), 0)
            self.assertEqual(before, (args.output_dir / "metadata_results.csv").read_bytes())

    def test_parallel_rate_limit_stops_other_workers_and_keeps_finished_success(self):
        args = e.parse_args(["--workers", "3"])
        pacer = e.RequestPacer(1)
        barrier = threading.Barrier(3)
        rows = []
        calls = []

        def extract(video_id, *_):
            calls.append(video_id)
            barrier.wait(timeout=5)
            if video_id == "limited":
                pacer.stop()
                return e.failure_row("abcdefghijk", "rate_limited", "HTTP 429")
            if video_id == "success":
                return e.metadata_row("ABCDEFGHIJK", video(filesize=100))
            pacer.pause(10)
            raise AssertionError("must stop waiting")

        with patch.object(e, "collect_one", side_effect=extract):
            status = e.collect_parallel(["limited", "success", "waiting", "never_started"], args, pacer, rows.append, lambda _: None)
        self.assertEqual(status, "rate_limited")
        self.assertEqual(set(calls), {"limited", "success", "waiting"})
        self.assertEqual({r["metadata_status"] for r in rows}, {"rate_limited", "success"})

    def test_parallel_ctrl_c_preserves_finished_work_and_allows_resume(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d)
            args.workers = 2

            def extract(video_id, *_):
                raise KeyboardInterrupt

            with patch.object(e, "collect_one", side_effect=extract):
                self.assertEqual(self.run_quietly(args), 130)
            summary = json.loads((args.output_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["run_status"], "interrupted")
            self.assertIn("`interrupted`", (args.output_dir / "report.md").read_text(encoding="utf-8"))
            self.assertFalse((args.output_dir / ".run.lock").exists())
            with patch.object(e, "collect_one", side_effect=lambda video_id, *_: e.metadata_row(video_id, video(filesize=100))):
                self.assertEqual(self.run_quietly(args), 0)
            self.assertEqual(len(e.load_latest(args.output_dir / "metadata_results.csv")), 3)

    def test_ctrl_c_writes_report_before_waiting_for_inflight_workers(self):
        with tempfile.TemporaryDirectory() as d:
            args = self.args(d, 1)
            with patch.object(e, "collect_one", side_effect=lambda video_id, *_: e.metadata_row(video_id, video(filesize=1_000_000))):
                self.assertEqual(self.run_quietly(args), 0)
            args.sample_size = 3
            args.workers = 2
            both_started = threading.Barrier(3)
            release = threading.Event()
            first_snapshot = []
            save_reports = e.save_reports

            def extract(video_id, *_):
                both_started.wait(timeout=5)
                if not release.wait(timeout=5):
                    raise AssertionError("report was not saved before waiting for workers")
                return e.metadata_row(video_id, video(filesize=1_000_000))

            def interrupt_main(*_args, **_kwargs):
                both_started.wait(timeout=5)
                raise KeyboardInterrupt

            def save(out, report):
                save_reports(out, report)
                if not release.is_set():
                    saved = json.loads((out / "summary.json").read_text(encoding="utf-8"))
                    first_snapshot.append(saved)
                    self.assertEqual(saved["run_status"], "interrupted")
                    self.assertEqual(saved["size_known_count"], 1)
                    self.assertIn("`interrupted`", (out / "report.md").read_text(encoding="utf-8"))
                    # A second real SIGINT during saving/cleanup must not abort it.
                    signal.raise_signal(signal.SIGINT)
                    release.set()

            try:
                with patch.object(e, "collect_one", side_effect=extract), patch.object(e, "wait", side_effect=interrupt_main), patch.object(e, "save_reports", side_effect=save):
                    self.assertEqual(self.run_quietly(args), 130)
            finally:
                release.set()
            self.assertEqual(len(first_snapshot), 1)
            final = json.loads((args.output_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(final["run_status"], "interrupted")
            self.assertEqual(final["size_known_count"], 3)
            self.assertEqual(final["size_statistics"]["mean_mb"], 1)
            self.assertFalse((args.output_dir / ".run.lock").exists())

    def test_pacing_is_global_across_threads(self):
        pacer = e.RequestPacer(0.03)
        starts = []
        def request():
            pacer.wait()
            starts.append(time.monotonic())
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda _: request(), range(3)))
        starts.sort()
        self.assertTrue(all(b - a >= 0.025 for a, b in zip(starts, starts[1:])))
        pacer.stop()
        with self.assertRaises(e.BatchStopped):
            pacer.wait()

    def test_warning_stops_shared_pacer(self):
        pacer = e.RequestPacer(1)
        with self.assertRaises(e.RateLimited):
            e.MetadataLogger(pacer).warning("HTTP 429: Too Many Requests")
        self.assertTrue(pacer.stopped.is_set())


def yt_error(message):
    return e.yt_dlp.utils.DownloadError(message)


class StatisticsTests(unittest.TestCase):
    def test_summary_denominators_and_decimal_units(self):
        rows = [e.metadata_row("abcdefghijk", video(filesize=1_000_000, duration=10)),
                e.metadata_row("123456789_-", video(filesize=3_000_000, duration=30)),
                e.metadata_row("ABCDEFGHIJK", video(duration=50)),
                e.failure_row("abcdefghij_", "failed", "missing")]
        s = e.build_summary(rows, {"valid_url_ratio": 0.8, "unique_video_ids": 10}, 5, 42, 56093, 1000, "completed", "test")
        self.assertEqual(s["size_statistics"]["mean_mb"], 2)
        self.assertEqual(s["size_statistics"]["mean_gb"], 0.002)
        self.assertEqual(s["duration_statistics"]["mean_seconds"], 20)
        self.assertEqual(s["metadata_success_rate"], 3 / 5)
        self.assertEqual(s["size_estimation_rate"], 2 / 5)
        self.assertEqual(s["processed_count"], 4)
        self.assertEqual(s["pending_count"], 1)
        self.assertEqual(s["estimation_method_counts"], {"exact": 2, "approximate": 0, "bitrate": 0, "unknown": 2})
        self.assertAlmostEqual(s["population_estimate"]["total_tb"], 2 * 1e6 * 56093 / 1e12)
        self.assertEqual(s["size_statistics"]["p90_mb"], 2.8)

    def test_empty_sample_has_no_fabricated_interval(self):
        s = e.build_summary([], {}, 3, 42, 56093, 100, "rate_limited", "test")
        self.assertIsNone(s["size_statistics"]["mean_mb"])
        self.assertIsNone(s["bootstrap_95_ci"]["mean_mb"])
        self.assertIsNone(s["population_estimate"]["total_tb"])
        json.dumps(s, allow_nan=False)

    def test_bootstrap_reproducible_and_single_sample_unknown(self):
        self.assertEqual(e.bootstrap_ci([1, 2, 100], 42, 1000), e.bootstrap_ci([1, 2, 100], 42, 1000))
        self.assertIsNone(e.bootstrap_ci([1], 42, 1000))


if __name__ == "__main__":
    unittest.main()
