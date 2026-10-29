"""Offline-only command line interface."""
import argparse
import csv
from pathlib import Path
import sqlite3
import sys

from .ingest import checked_data_dir, import_existing
from .reporting import write_report


def main(argv=None):
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="RMB YouTube Controller：僅本機離線匯入，無網路或下載功能")
    parser.add_argument("command", choices=("import-existing", "report"))
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--data-dir", type=Path, help="必須位於 <project-root>/controller/ 內；預設 controller/data")
    args = parser.parse_args(argv)
    committed = False
    try:
        root = args.project_root.resolve()
        data_dir = checked_data_dir(root, args.data_dir)
        if args.command == "import-existing":
            import_id, data_dir = import_existing(root, data_dir)
            committed = True
            print(f"離線匯入已提交：{import_id}")
        target, summary = write_report(data_dir)
        print(f"SQLite：{data_dir / 'controller.sqlite3'}")
        print(f"唯一影片：{summary['video_count']:,}；既有本機完成：{summary['download_status_counts'].get('completed_local', 0):,}")
        print(f"Metadata 狀態：{summary['metadata_status_counts']}")
        print(f"報告：{target / 'report.md'}")
        return 0
    except (OSError, ValueError, sqlite3.Error, csv.Error) as exc:
        print(f"錯誤：{exc}", file=sys.stderr)
        if committed:
            print("資料庫匯入已提交；可修復報告目錄後執行 report，不需重做匯入。", file=sys.stderr)
        return 1
