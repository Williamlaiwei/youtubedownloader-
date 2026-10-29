"""Build/update VMtask from saved data while metadata collection is paused."""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from rmb_controller.vmtask import export_vmtask, remove_legacy_samples, validate_vmtask


def main():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="整理既有 metadata 與壞連結紀錄到 VMtask，拆成 20 batch；不採集、不下載")
    parser.add_argument("--remove-legacy-samples", action="store_true", help="確認 JSON 遷移後刪除原本含影片的樣例資料夾")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--consolidate", action="store_true", help="壓縮保存舊 history／未完成匯出，合併成固定 20 batch")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    if args.validate_only:
        result = validate_vmtask(root / "VMtask")
    else:
        result = export_vmtask(root, consolidate=args.consolidate)
        if args.remove_legacy_samples:
            result["cleanup"] = remove_legacy_samples(root)
    summary = {k: v for k, v in result.items() if k not in {"batches", "record_hashes", "distribution_hashes"}}
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
