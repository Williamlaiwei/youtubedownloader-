# 本機 metadata 採集監視器

雙擊 `controller/open_monitor.cmd`，或從專案根目錄執行：

```powershell
.\estimate\youtube_size_estimator\.venv\Scripts\python.exe -B .\estimate\youtube_size_estimator\estimate.py --monitor --workers 3 --request-interval 1
```

監視器開啟後自動續跑，採集工作屬於這個可見視窗；不使用排程、隱藏常駐程序、模型或 API 金鑰。
目前視窗標題帶有 `[SQLite v3]`，方便辨識是否仍開著舊版程式。啟動或續跑失敗時，完整 traceback、程式路徑、資料庫路徑、支援與實際 schema 版本會記錄到 `controller/data/logs/monitor_errors.jsonl`；不會為了通過檢查而重建或降版資料庫。
目前預設使用已實測的網頁輕量模式。必要基本欄位缺失時，最多回退一次完整模式；仍取不到就保留缺值，列入 held_tasks，不假填 0。新資料的解析度可為 null，resolution_status=pending_vm 存在 SQLite／tasks 中，VM 通過 ffprobe 後補齊；舊資料不清除。

- 「已查」以新的 `collection_attempts` 中唯一 video_id 計算，不拿舊 500 支抽樣冒充本次進度。
- 「已存原始 metadata」包含完整度不足的回應；缺少 like_count 等欄位仍保持 null，另外列出缺欄位數。
- 讀取整份 56,093 個 ID，包含補查原有 20 支樣例的原始 metadata。已完成的新採集結果不重複查詢。
- 「並行數」在採集中也能調整（1–4），改完立即寫入 SQLite，採集迴圈每輪讀取。調高會馬上補送請求；調低不中斷已送出的請求，待其完成後才減少。
- 「共用請求間隔」（1–30 秒）同樣可在採集中調整，所有並行共用同一個間隔。調短時，已排定的下一次請求最多等舊間隔一次；調長則從下一次請求開始套用。
- 整理 VMtask 只會更新**同一個** `VMtask/`。暫存的快照固定放在 `controller/data/vmtask_stage/`，每次整理前清掉、結束後刪除，不會在專案根目錄產生 `.vm-stage-*`；舊版殘留的會在下次整理時自動清除。
- 若 VMtask 內有檔案正被 Excel／WPS 等程式開啟，整理會在**動手前**就停下並列出檔名，VMtask 維持上一版（READY.json 保留、仍可派發）。關閉那些檔案後再整理即可。
- 「停止並保存」停止新請求，等待在途請求結束並落盤。按「開始／續跑」接著處理尚未完成者。
- 關閉視窗採同樣的停止流程，保存後才退出；請等待視窗自行關閉。
- 429 會停止本輪並保存進度，設定自動續跑倒數。可隨時按「立即嘗試續跑」測試一支，成功後繼續；若仍是 429 就停止，不連續重試。暫時錯誤不會刪除 URL，也不會被當成永久失效。
- 每輪遇到 HTTP 429 會跳出一次「YouTube HTTP 429 限流」通知；通知不阻擋進度保存或 VMtask 整理。重新開啟時若仍在冷卻，會明確寫「上次採集」，不代表剛發出新請求。
- 資料庫版本錯誤顯示「資料庫版本不相容」；整理 CSV／任務包失敗顯示「VMtask 更新失敗」。採集結果與整理結果分開記錄，即使整理失敗也保留原本的 429 狀態及進度。完整錯誤日誌會標示 collection／publication 階段。
- 不下載影片、音訊、縮圖或字幕；不連線 VM 或 OSS。
- 匯出時同步打包 20 份 `VMpackages/packages/batch-XX.tar.gz`，每份均含對應任務及相同版本 VM Worker；尚未採完的 ID 仍在 pending_metadata.csv。

原有 `estimate.py` 不加 `--monitor` 時仍保留原本的抽樣估算行為。
修改前的程式備份於 `controller/data/backups/estimate-before-monitor-*.py`，既有 output、CSV、影片樣例不變。

## 輸出位置

```text
controller/data/
├── raw_metadata/<video_id>/<版本>.json     # 完整 yt-dlp 公開欄位，經 sanitize_info JSON 序列化
├── metadata/<video_id>/<版本>.json         # 客戶格式，缺值不假造
├── controller.sqlite3                     # 進度、每次嘗試、錯誤分類、版本雜湊
└── catalog/<固定目錄名稱>/metadata.json     # 已產生 metadata 的目前工作副本
```

原始回應與客戶 JSON 使用不同版本檔名，寫入後不覆寫。catalog 是可更新的工作副本；原始樣例不會改名。
每支採集完成就立即更新 `catalog/<影片標題>/metadata.json`，不必等待整輪結束。監視器的「開啟標題資料夾」可直接查看；雙擊最近結果也能開啟該影片的標題資料夾。內部 raw_metadata、metadata 的 video_id 目錄保留作版本索引。

已建立 VMtask 後，「開啟任務資料夾」會開啟 VMtask。停止或完成採集時，將新增資料合併到固定的 batch-01～batch-20，不搬走既有目錄，不再產生 `_history`。VMtask 只有這 20 個資料夾與統計、紀錄文件；VM 程式及上傳壓縮包在旁邊的 `VMpackages/`。被替換的舊文件以 ZIP 保存在 `controller/data/vmtask_archive/`，原始 metadata 與 SQLite 歷史仍保留。

最新不可用／暫時錯誤即時寫入 `VMtask/bad_files_live_failures.jsonl`，`bad_files_current.csv` 和 `bad_files_retryable.csv` 隨任務包更新。尚在採集中的逐支結果仍即時保存在 catalog；VMtask 清單於停止／完成採集時合併。未完成者不會假裝成已準備好的 VM 任務。更新期間暫時移除 READY.json，完成並通過驗證後才恢復，勿在此期間複製派發。
SQLite 以 video_id 為識別，程式會阻止兩個採集寫入程序同時操作。

metadata 處理仍重用原有 `estimate.py` 的 `MetadataOnlyYDL`、請求節流器、格式選擇與容量計算。監視器只是桌面控制介面。
