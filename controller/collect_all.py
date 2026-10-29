"""Collect metadata for every remaining URL. Never downloads media."""
import argparse
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from rmb_controller.artifacts import controller_lock
from rmb_controller.metadata import collect_metadata


def main():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="逐一取得 TXT 清單全部網頁的 metadata；不下載影片")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    data = root / "controller/data"
    with controller_lock(data):
        result = collect_metadata(root, data, limit=args.limit, workers=args.workers,
                                  interval=args.interval, timeout=args.timeout,
                                  retries=args.retries, all_pages=True)
    print(result, flush=True)
    return 0 if result["status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
