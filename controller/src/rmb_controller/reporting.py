"""Read-only database reports, written to a fresh directory for every invocation."""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

from .database import connect_database
from .ingest import json_text, utc_now


def counts(db, column):
    # Column names are fixed by callers, never supplied by the CLI.
    return {row[0]: row[1] for row in db.execute(f"SELECT {column},COUNT(*) FROM videos GROUP BY {column}")}


def build_report(data_dir):
    data_dir = Path(data_dir).resolve()
    db = connect_database(data_dir / "controller.sqlite3", readonly=True)
    try:
        db.execute("BEGIN")
        latest = db.execute("SELECT * FROM import_runs WHERE completed_at_utc IS NOT NULL ORDER BY rowid DESC LIMIT 1").fetchone()
        if latest is None:
            raise ValueError("No completed import exists")
        import_id = latest["import_id"]
        sources = [dict(row) for row in db.execute("""SELECT s.source_id,s.relative_path,s.sha256,
            s.size_bytes,s.kind,s.snapshot_path FROM source_files s JOIN run_sources r USING(source_id)
            WHERE r.import_id=? ORDER BY s.relative_path""", (import_id,))]
        inputs = []
        estimator = None
        for source in sources:
            if source["kind"] == "url_list":
                line_counts = dict(db.execute("SELECT outcome,COUNT(*) FROM source_lines WHERE source_id=? GROUP BY outcome",
                                              (source["source_id"],)))
                inputs.append({"path": source["relative_path"], "sha256": source["sha256"],
                               "total_lines": sum(line_counts.values()), "outcomes": line_counts,
                               "unique_video_ids": db.execute("SELECT COUNT(DISTINCT video_id) FROM source_lines WHERE source_id=?",
                                                               (source["source_id"],)).fetchone()[0]})
            if source["kind"] == "estimator_results":
                rows = db.execute("""SELECT imported_status,error_code FROM metadata_versions
                    WHERE source_id=? AND record_index IN
                    (SELECT MAX(record_index) FROM metadata_versions WHERE source_id=? GROUP BY video_id)""",
                    (source["source_id"], source["source_id"])).fetchall()
                estimator = {"path": source["relative_path"],
                             "history_records": db.execute("SELECT COUNT(*) FROM metadata_versions WHERE source_id=?",
                                                           (source["source_id"],)).fetchone()[0],
                             "unique_video_ids": len(rows),
                             "latest_imported_status_counts": dict(Counter(row["imported_status"] for row in rows)),
                             "latest_failure_codes": dict(Counter(row["error_code"] for row in rows
                                                                     if row["imported_status"] != "success"))}
        issues = [dict(row) for row in db.execute("""SELECT v.video_id,v.url,v.original_title,
            v.folder_name,v.metadata_status,v.download_status,v.error_code,v.error_message,
            m.missing_fields_json,s.relative_path AS source_path,m.record_index
            FROM videos v JOIN metadata_versions m ON v.current_metadata_version=m.metadata_version_id
            JOIN source_files s USING(source_id) WHERE v.metadata_status NOT IN ('ready','pending')
            ORDER BY v.video_id""")]
        for issue in issues:
            issue["missing_fields"] = json.loads(issue.pop("missing_fields_json"))
        summary = {
            "schema_version": 1, "generated_at_utc": utc_now(), "mode": "offline_existing",
            "latest_import_id": import_id, "database_path": str(data_dir / "controller.sqlite3"),
            "project_root": latest["project_root"], "video_count": db.execute("SELECT COUNT(*) FROM videos").fetchone()[0],
            "metadata_status_counts": counts(db, "metadata_status"),
            "download_status_counts": counts(db, "download_status"),
            "source_files_this_import": len(sources),
            "source_versions_total": db.execute("SELECT COUNT(*) FROM source_files").fetchone()[0],
            "source_lines_total": db.execute("SELECT COUNT(*) FROM source_lines").fetchone()[0],
            "evidence_records_total": db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0],
            "metadata_versions_total": db.execute("SELECT COUNT(*) FROM metadata_versions").fetchone()[0],
            "known_title_count": db.execute("SELECT COUNT(*) FROM videos WHERE original_title IS NOT NULL").fetchone()[0],
            "known_estimated_size_count": db.execute("SELECT COUNT(*) FROM videos WHERE estimated_size_bytes IS NOT NULL").fetchone()[0],
            "registered_folder_count": db.execute("SELECT COUNT(*) FROM videos WHERE folder_name IS NOT NULL").fetchone()[0],
            "completed_local_bytes": db.execute("SELECT COALESCE(SUM(actual_size_bytes),0) FROM videos WHERE download_status='completed_local'").fetchone()[0],
            "assigned_video_count": db.execute("SELECT COUNT(*) FROM videos WHERE assigned_part IS NOT NULL").fetchone()[0],
            "issue_count": len(issues), "url_inputs": inputs, "estimator": estimator,
            "sources": sources,
            "limitations": [
                "本階段只匯入既有本機證據，未發出網路請求或下載影片。",
                "metadata ready 只表示既有客戶 JSON 通過基本欄位檢查，並非重新採集或交付 QC。",
                "like_count=null 保持缺值並標為 partial；未假造 0，待客戶缺值政策確認。",
                "completed_local 來自現有非空媒體檔；本次未重跑 ffprobe，也未計算影片內容雜湊。",
                "attempts 是歷史證據列，可能重複描述同一次嘗試，不能用列數當作網路請求或重試次數。",
                "failed_urls.csv 只作為歷史證據，當前狀態依 metadata_results.csv 的最新列還原。",
                "本階段沒有建立標題目錄、tasks.jsonl、任務包或 VM Worker。",
                "report 指令反映資料庫快照，不重新檢查原始檔案或媒體是否改變。",
            ],
        }
        return summary, issues
    finally:
        db.close()


def write_report(data_dir):
    summary, issues = build_report(data_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
    target = Path(data_dir) / "reports" / stamp
    target.mkdir(parents=True, exist_ok=False)
    status_lines = "\n".join(f"| {key} | {value:,} |" for key, value in summary["metadata_status_counts"].items())
    inputs = "\n".join(f"- `{x['path']}`：{x['total_lines']:,} 行、{x['unique_video_ids']:,} 個唯一 ID；{x['outcomes']}"
                       for x in summary["url_inputs"])
    limitations = "\n".join("- " + value for value in summary["limitations"])
    markdown = f"""# RMB YouTube Controller：離線匯入報告

匯入批次：`{summary['latest_import_id']}`  
報告時間（UTC）：{summary['generated_at_utc']}

## 目前資料

- 唯一影片：{summary['video_count']:,}
- 已知標題：{summary['known_title_count']:,}
- 已知容量估算：{summary['known_estimated_size_count']:,}
- 沿用既有目錄：{summary['registered_folder_count']:,}
- 已完成本機樣例：{summary['download_status_counts'].get('completed_local', 0):,}
- 本機樣例總 bytes：{summary['completed_local_bytes']:,}
- 已分配 part：{summary['assigned_video_count']:,}

## Metadata 狀態

| 狀態 | 數量 |
|---|---:|
{status_lines}

## 輸入清單

{inputs}

## 來源與歷史

- 本批匯入來源檔：{summary['source_files_this_import']:,}
- 原始 TXT 行紀錄：{summary['source_lines_total']:,}
- 歷史證據列：{summary['evidence_records_total']:,}（不是實際請求次數）
- Metadata 觀察版本：{summary['metadata_versions_total']:,}
- 待補齊或分類問題：{summary['issue_count']:,}，明細見 `issues.jsonl`。
- 所有來源 SHA-256 與快照路徑見 `report.json`。

## 範圍與限制

{limitations}
"""
    for filename, content in (("report.json", json_text(summary) + "\n"),
                              ("report.md", markdown),
                              ("issues.jsonl", "".join(json_text(row) + "\n" for row in issues))):
        with (target / filename).open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
    return target, summary
