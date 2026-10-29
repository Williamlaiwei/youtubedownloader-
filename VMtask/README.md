# VMtask：固定 20 個 batch

已整理 1,074 份 metadata；可派發 1,019 支，暫留 55 支。
總 URL 56,093 支，尚有 55,019 支沒有客戶格式 metadata。

- batch-01～batch-20 是唯一的一組任務目錄。新增資料合併到原本 batch，不搬走或重建目錄。
- tasks.jsonl 的 folder_name 與 videos/<影片標題>/metadata.json 完全一致；VM 不可重新命名。
- held_tasks.jsonl 保留必要欄位缺值或不可用的項目，不自動下載。
- metadata.json 的 files=["001.mkv"] 是 VM 預定檔名；本機不下載影片。
- 輕量採集的解析度可為 null；VM 下載最高畫質並通過 ffprobe 後更新解析度。
- inventory.csv 是完整清單，pending_metadata.csv 是待採集，metadata_warnings.jsonl 是缺欄位紀錄。
- bad_files_current.csv 是不可用紀錄；只有 permanent=true 為明確永久失效。
- bad_files_retryable.csv 記錄網路錯誤、429 與中斷；不刪除 URL。
- bad_files_history.jsonl 與 bad_files_live_failures.jsonl 保留歷史及即時失敗。
- source_manifest.json 對應 source_records.zip 內的原始 TXT、抽樣與下載紀錄。
- VM 程式和 20 份獨立上傳壓縮包放在專案的 VMpackages/，不佔用此處的資料夾。
- 舊版本與被更新的檔案壓縮保存在 controller/data/vmtask_archive/，不在此處新增歷史 batch。
- 更新期間沒有 READY.json；READY.json 與 manifest.json 一致才可派發。複製任務包請在更新完成後進行。
