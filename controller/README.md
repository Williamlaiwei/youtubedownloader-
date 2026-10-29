# RMB YouTube Pipeline V1：本機 Controller

目前提供離線匯入，以及由原有 `estimate.py --monitor` 開啟的**可見本機 metadata 採集監視器**。

雙擊 [open_monitor.cmd](open_monitor.cmd) 即可開啟視窗並續跑全部清單。視窗顯示已查頁數、已存原始 metadata、錯誤分類、速度與最近結果，提供「停止並保存」和「開始／續跑」。關閉視窗會停止採集並等待保存；不會留下獨立的背景採集程序。詳見 [METADATA_MONITOR.md](METADATA_MONITOR.md)。

使用 Python 3.12+ 標準函式庫，不需要 pip 安裝或沿用估算器的虛擬環境。
離線匯入與任務包匯出只需標準函式庫；監視器使用估算器既有虛擬環境及 yt-dlp。採集不使用模型，不下載媒體，不連接 VM 或 OSS。

## 整理 VMtask（採集保持暫停）

```powershell
python -B .\controller\export_vmtask.py
python -B .\controller\export_vmtask.py --validate-only
```

`VMtask/batch-01` 至 `batch-20` 各有 `tasks.jsonl`、`held_tasks.jsonl`、`manifest.json` 及 `videos/<固定標題>/metadata.json`。
主清單只含欄位完整者，缺值 JSON 也會保存並列入 held_tasks；不包含 `.controller.json`。總清單涵蓋全部 URL，未採完者在 `pending_metadata.csv`。

`bad_files/current.csv` 保存目前無法取得的 URL 及原因；只有 `permanent=true` 才是明確永久失效。暫時網路錯誤、429、中斷在 `bad_files/retryable.csv`，歷史失敗在 `bad_files/history.jsonl`。
原始 TXT、抽樣與下載紀錄快照在 `records/sources/`，以 `records/source_manifest.json` 對照原始路徑。

再次匯出保留固定 batch 分配和既有目錄，新增 metadata 合併進同一組 20 batch；被更新的舊文件壓縮保存到 `controller/data/vmtask_archive/`。若現有批次已被加入影片或檔案被改動，會拒絕覆寫。舊版 `_history` 或搬移到一半的資料可用 `controller/export_vmtask.py --consolidate` 驗證、壓縮保留後整理。
`--remove-legacy-samples` 僅在明確需要清除舊樣例時使用：先逐一驗證 JSON 遷移雜湊及原始影片大小、修改時間，再刪除原 supplier 目錄下的兩檔樣例。其他資料不刪除，操作紀錄保存在 `records/legacy_sample_cleanup.jsonl`。

## 執行

從 `youtube_crawl` 根目錄：

```powershell
python -B .\controller\run.py import-existing
```

輸出 SQLite 與新報告的位置；報告使用 UTC 時間及隨機識別，不覆寫舊報告。

```powershell
# 只讀取已匯入的 SQLite，另產生一份報告
python -B .\controller\run.py report

# 離線測試；使用暫存專案，不操作原始資料
python -B -m unittest discover -s .\controller\tests -v
```

也可使用估算器既有的 Python 3.12：

```powershell
.\estimate\youtube_size_estimator\.venv\Scripts\python.exe -B .\controller\run.py report
```

`--project-root` 可指定專案根目錄。`--data-dir` 可指定新的資料目錄，但必須在該專案的 `controller/` 之下，避免寫入原始資料區。

## 會讀取的資料

- `videos/youtube_urls.txt` 與 `estimate/youtube_urls.txt`：兩份都保存來源、原始行號、原始文字及解析結果。
- `estimate/youtube_size_estimator/output/metadata_results.csv`：依列順序還原每個 ID 的最新狀態，保留全部歷史列。
- 同目錄 `failed_urls.csv`：只匯入歷史證據，不用來蓋掉後來成功的結果。
- `download_record/download_log.csv`、`estimate/sample20/results.json`：保留下載歷史與原始量測。
- `SUPPLIER-v1-1028-youtube_camera_control/*/metadata.json`：保存客戶 JSON，檢查 ID、URL、資料夾與現有非空影片檔。
- 上述四個既有資料夾中的其他非媒體檔：保存來源快照，例如需求 PDF、程式、報告及 manifest。

排除虛擬環境、快取、版本控制資料與影片內容。所有來源必須留在專案內。
影片只讀取檔案大小、修改時間；不複製數 GB 影片，不重新下載，也不把存在檔案當成已通過 QC。

## 資料與保存方式

```text
controller/
├── run.py
├── src/rmb_controller/
├── schemas/schema_v1.sql
├── tests/
└── data/
    ├── controller.sqlite3
    ├── imports/objects/<sha256>.blob
    └── reports/<UTC時間>-<識別>/
        ├── report.md
        ├── report.json
        └── issues.jsonl
```

`.blob` 是原始非媒體檔案的逐位元組快照，不是 yt-dlp 原始回傳。來源路徑、SHA-256、快照位置在 `source_files` 及報告中；相同內容可共用快照。

- 不修改、不搬動、不覆寫任何原始檔案、目錄或樣例。
- 相同來源路徑和內容再次匯入，不增加影片、來源行、metadata 或證據列；每次成功執行另記一筆匯入批次。
- 來源內容改變時新增快照及歷史版本，不覆寫舊版本；已匯入的 ID 不會因來源後來移除而被刪除。
- 每批匯入使用單一 SQLite 交易；解析、名稱衝突、來源改動或寫入失敗時整批回滾。失敗時可留下未引用的來源快照，不會覆寫或自動刪除它們。
- SQLite 交易序列化匯入寫入。重新匯入不重設 `assigned_part`。
- 若資料庫已提交但報告輸出失敗，修復輸出位置後執行 `report` 即可。
- `report` 是資料庫快照報告，不重新驗證來源檔案是否仍存在。

## 狀態的意思

| 欄位／狀態 | 意義 |
|---|---|
| `video_id` | 大小寫有別的唯一識別，不以標題辨識影片 |
| `metadata_status=pending` | 沒有匯入 metadata 觀察結果 |
| `partial` | 有部分資料，尚未滿足客戶基本欄位要求 |
| `ready` | 既有客戶 JSON 通過基本欄位檢查，並非 VM 已可執行或重新 QC |
| `restricted` | 私人、地區、年齡或登入限制，與影片刪除分開 |
| `permanent_failure` | 保存訊息有明確移除／刪除證據，仍保留 URL |
| `insufficient` | unavailable、404、403、無格式等不足以判定永久失效的情況 |
| `retry_wait`、`rate_limited`、`interrupted` | 可供後續排程恢復；此版本不會實際重試 |
| `download_status=completed_local` | 已找到既有非空影片檔，不代表本批執行過下載或 QC |
| `assigned_part=NULL` | 尚未規劃 20 份包 |
| `estimated_size_bytes=NULL` | 未知，不當成 0；實際檔案大小另存 `actual_size_bytes` |
| `retry_count` | 沿用 metadata CSV 累計 attempt_count 減一，下限為 0；不拿歷史列數推算 |

客戶 `like_count=null` 保持缺值、標為 `partial`；`0` 是有效計數。缺值是否可交付仍待後續政策確認。
既有客戶 JSON 的完整度優先於估算器 CSV；同一來源類別按觀察時間、來源版本、列順序選擇目前資料。
估算器 CSV 沒有 description、author、author_url、view_count、like_count 等欄位，不能把其 success 當成完整客戶 metadata。
目前沒有匯入完整 yt-dlp 原始 metadata。

## 目錄名稱

只登記現有 20 個樣例目錄的實際名稱，設定 `naming_version=legacy_preserved`。
實際名稱不做 Unicode 正規化；另用 NFC + casefold 的比較鍵檢查衝突。
SQLite 觸發器禁止默默改動已登記名稱；發現目錄改名或衝突時回滾。

此版本尚不從標題產生新目錄，因此不會提前決定同名後綴、截短規則或客戶例外。
下一個里程碑再建立命名政策、完整 metadata 驗證與任務包匯出。未來 VM 必須直接使用 `tasks.jsonl.folder_name`。

## SQLite 表

- `videos`：一個 ID 一筆目前狀態，含 URL、原始標題、固定目錄、metadata／下載狀態、part、容量、錯誤與重試次數。
- `source_files`、`source_lines`：來源版本與原始 TXT 行；空行、非法行與重複行也保留。
- `import_runs`、`run_sources`：每次成功匯入及其來源清單。
- `metadata_versions`：每個 metadata CSV 歷史列及客戶 JSON 的不可覆寫觀察版本。
- `attempts`：metadata 與下載歷史證據；多份來源可能描述同一次嘗試，**不能用筆數當成請求次數**。
- `local_media`：既有檔案位置、大小與修改時間的觀察紀錄。

資料庫目前為 schema v3；從本專案 schema v1/v2 升級時會先保留 SQLite 備份。未知資料庫或其他版本會被拒絕，不會覆寫。輕量模式只新增 metadata_mode 與 resolution_status，保留所有 ID、狀態、對應及歷史。

目前預設採集網頁基本 metadata；基本欄位取不到時有限回退一次完整模式，仍缺值就暫留，不假造資料。解析度在 VM 下載並由 ffprobe 驗證後補入。
VMtask 固定保留 batch-01～batch-20，新增 metadata 直接合併，未變動的檔案不重寫。統計與錯誤紀錄平放在 VMtask；不在此處建立歷史 batch。舊版本壓縮保存在 `controller/data/vmtask_archive/`，原始資料及 SQLite 歷史保留。

每次匯出會同步產生 `VMpackages/packages/batch-01.tar.gz` 至 `batch-20.tar.gz`，各包含該 batch 與 `worker/`，以及 `VMpackages/vm_worker_bundle.tar.gz`。不要把尚未完成整批採集的資料誤認為全部 56,093 支都已備妥。
