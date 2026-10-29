# RMB VM Worker V1

本機取得基本 metadata，VM 只處理下載、合併、QC、更新解析度，以及明確啟用後的 TAR 串流上傳。程式不使用模型。
同時支援原 tasks schema 1 與新的 schema 2。固定使用 tasks.jsonl 的 folder_name，不從 title 重新產生目錄。

## 一份上傳包的執行方式

本機 `VMpackages/packages/batch-01.tar.gz` 已包含 `batch-01/` 和 `worker/`，可選任一 batch 給 VM。`VMtask/` 固定只有一組 20 batch 與統計文件；程式及上傳壓縮包另放 `VMpackages/`。不要把同一 batch 同時交給兩台 VM。

```bash
mkdir -p ~/rmb && cd ~/rmb
tar -xzf batch-01.tar.gz
```

### Docker（包含 Python、Node 22、ffmpeg）

```bash
docker build -t rmb-worker:v1 ./worker

# 先只驗證任務，不抓影片、不連 OSS
docker run --rm -v "$PWD/batch-01:/tasks:ro" rmb-worker:v1 --batch /tasks --validate-only

# 實際下載；中斷後使用完全相同命令續跑
mkdir -p "$PWD/output"
docker run --rm -it \
  -v "$PWD/batch-01:/tasks:ro" -v "$PWD/output:/output" \
  rmb-worker:v1 --batch /tasks --output-root /output
```

### 已有 Python 的 Linux VM

需要 Python 3.11+、ffmpeg/ffprobe、Node >=22 或 Deno >=2.3（在 PATH）。

```bash
python3 -m venv worker/.venv
worker/.venv/bin/pip install -r worker/requirements.txt
worker/.venv/bin/python worker/worker.py --batch batch-01 --validate-only
worker/.venv/bin/python worker/worker.py --batch batch-01 --output-root ./output
```

四台 VM 可各處理 5 個不同 batch；先以單支確認 VM 環境，再處理整批：

```bash
worker/.venv/bin/python worker/worker.py --batch batch-01 --output-root ./output --limit 1
```

## 輸出與續跑

```text
output/<export_id>/batch-01/
├── videos/<原 folder_name>/
│   ├── 001.mkv
│   └── metadata.json
└── .worker/
    ├── state.sqlite3
    ├── summary.json
    ├── results.csv
    ├── events.jsonl
    ├── failures.jsonl       # 有失敗才建立
    ├── uploads.jsonl        # 有上傳才建立
    └── downloads/           # 保留尚未完成的 .part，供續跑
```

- 輸入 batch 唯讀，JSON 與 task list 都先驗證雜湊。缺欄位的 held_tasks.jsonl 不會下載。
- 每支僅用一次 yt-dlp extract_info(download=True) 同時選格式及下載；固定 bestvideo*+bestaudio/best，依 res/fps 排序，不設解析度上限，不嘗試較低畫質來掩蓋錯誤。
- ffprobe 核對所選與實際解析度、正時長、預期音軌。失敗的檔案不發布、不上傳。
- 客戶 metadata 只更新 resolution、width、height；描述、作者、觀看數、點讚數及本機採集時間保持不變。
- 已完成結果續跑不重抓影片；預設依大小、修改時間與 metadata 雜湊確認，`--verify-hashes` 可額外重算影片雜湊。
- `--retry-failed` 明確重試已失敗項目；一般續跑先保留其錯誤。429 停止整批；暫時錯誤不當作永久失效。
- Ctrl+C 保存 SQLite 與 .part 後退出；輸出不能覆寫成不同任務快照。
- 沒有 --upload 時，影片留在 VM 磁碟，請預留足夠容量。

## TAR 串流上傳 Akamai S3-compatible OSS（預設關閉）

僅適用 Akamai Object Storage 的 S3 相容 endpoint。endpoint、region、bucket 和憑證必須在 VM 填入，程式包不含真實金鑰。
上傳使用 boto3、S3 v4、path addressing、when_required checksum 設定以避開不相容的 AWS 預設 checksum header；不同 Akamai endpoint 仍需 VM 小檔實測。

```bash
cp worker/oss.env.example ./oss.env
# 在 VM 修改 oss.env，再載入環境變數
set -a
. ./oss.env
set +a

docker run --rm -it --env-file ./oss.env \
  -v "$PWD/batch-01:/tasks:ro" -v "$PWD/output:/output" \
  rmb-worker:v1 --batch /tasks --output-root /output \
  --upload --chunk-gib 20 --delete-after-upload
```

- 每累積約 20 GiB 已通過 QC 的影片，就將該組影片以 TAR 串流送到 OSS；不另落地大型 TAR。單支影片可能讓分組超過這個值。
- TAR 中保留客戶外層目錄與每支原標題資料夾，僅含 metadata.json 和 001.mkv。
- 每段以檔案雜湊生成固定 object key；已存在的相同 fingerprint 可續用，不覆寫不同內容。
- multipart 緩衝有上限，失敗會保留已完成的本機影片，下次可重新上傳而不重抓影片。
- --delete-after-upload 只有在 OSS 上傳完成且 HEAD 大小核對通過後，才刪除該段兩個確定檔案及空資料夾。預設不刪；請只在接受此行為時使用。
- batch.complete=true 表示所有 tasks.jsonl 已完成；分段上傳可先於整批完成，錯誤及剩餘項目仍會出現在 summary/results。

目前僅完成離線測試與本機合成影片測試。沒有連接 VM／OSS，也未實測 Akamai endpoint、憑證、VM 頻寬或批次下載吞吐。

參考：
- https://github.com/yt-dlp/yt-dlp/wiki/EJS
- https://ffmpeg.org/ffprobe.html
- https://techdocs.akamai.com/cloud-computing/docs/using-the-aws-sdk-for-python-boto3-with-object-storage
