# YouTube 大批量影片儲存容量估算

以 Python 3.11+ 與 yt-dlp Python API，從 TXT 中有效且不重複的影片 ID 隨機抽樣，只取得 metadata，估算最高可取得畫質所需容量。報告使用繁體中文。預設抽樣 500 支、seed 42，以 56,093 支作為外推母體。

## Windows PowerShell 使用

目前的 `youtube_urls.txt` 在本專案資料夾的上一層，程式會自動找到，不必搬移。以下指令從原始工作目錄執行：

```powershell
cd .\youtube_size_estimator

# 新環境才需要建立；本次交付已建立 .venv 並安裝相依套件
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade -r requirements.txt

# 全離線單元測試，不連線、不下載媒體
.\.venv\Scripts\python.exe -m unittest discover -s tests -v

# 小規模實測：只查詢 3 支
.\.venv\Scripts\python.exe estimate.py --sample-size 3 --seed 42

# 完整抽樣 500 支；沿用此次 3 支結果，成功項目不重查
.\.venv\Scripts\python.exe estimate.py --sample-size 500 --seed 42
```

直接呼叫虛擬環境 Python，不需要啟用腳本，也不需要更改 PowerShell ExecutionPolicy。已有 Python 3.11+ 時，可將建立環境的 `py -3.12` 改為自己的 Python 版本。

此次已檢查原始 TXT：56,093 行，全部通過 URL 結構驗證且 ID 不重複。3 支實測有 2 支 metadata 成功並取得精確串流大小，1 支為私人影片；未遇到限流、未下載媒體。已知大小平均 325.274 MB，僅供流程驗證，完整數值與小樣本限制見 `output/report.md`。另以阻擋網路呼叫的方式驗證續跑，沒有重新查詢已完成項目。

預設單工，每個 HTTP metadata 請求的開始時間至少相隔 2 秒；每支影片可能需要多次請求，因此 500 支可能需要較長時間。可用 `--workers 3 --request-interval 1` 加速，讓三支影片的網路與解析工作交錯進行。所有工作共用同一個請求節流器，全部工作合計最多每秒開始一次 HTTP 請求，並非每個工作各發一次；實際加速幅度取決於網路、回應及播放器腳本解析時間，未保證三倍加速。

按 Ctrl+C 會停止新請求，**立即以目前已保存的 CSV 更新 `output/report.md` 和 `output/summary.json`**，不需要等到 500 支完成。多工時，接著等待已送出的 HTTP 請求返回或逾時，保存完成的結果，再更新一次報告後退出。清理與報告寫入期間會忽略重複的 Ctrl+C，讓檔案完整保存。只有主執行緒寫入 CSV，完成項目立即 flush/fsync。重新執行相同指令即 resume，不需要 `--resume`。工作數與請求間隔可在續跑時調整，不會改變抽樣名單或重查成功項目。

遇到 HTTP 429、Too Many Requests、rate-limited 或其他明確限流／暫時封鎖訊息，立即停止整批、保存當前錯誤及統計，結束碼為 2。請等待服務恢復後自行重跑相同指令；程式不會在同一次批次內重試限流項目。不使用 Cookie、帳號、netrc、代理、IP 輪換、驗證碼繞過或 bot challenge 繞過。

多工時，任一工作遇到限流就會通知所有工作停止新請求、取消尚未開始的工作並中斷重試等待。已送出的 HTTP 請求無法撤回；若它們已完成取得 metadata，仍會保存結果，其他未完成的 URL 留待續跑。請只執行一個採集程序並使用 `--workers` 設定並行數，避免開多個視窗同時寫入相同 output。共用的請求間隔只在同一個程序內生效。

## 常用參數

```powershell
# 加速續跑：3 支影片並行，所有工作合計每秒最多開始 1 次 HTTP 請求
.\.venv\Scripts\python.exe estimate.py --sample-size 500 --seed 42 --workers 3 --request-interval 1

# 明確指定輸入；路徑有空白時請加引號
.\.venv\Scripts\python.exe estimate.py --input "..\youtube_urls.txt" --sample-size 500 --seed 42

# 只根據既有 CSV 更新報告，完全不連線；sample-size 必須與既有設定一致或更大
.\.venv\Scripts\python.exe estimate.py --sample-size 3 --seed 42 --report-only

# 一般失敗預設不再查詢；需要時明確啟用重試，保留所有歷史列
.\.venv\Scripts\python.exe estimate.py --sample-size 500 --seed 42 --retry-failed

# 新 seed、不同母體或另一個獨立實驗，使用另一個輸出資料夾
.\.venv\Scripts\python.exe estimate.py --sample-size 500 --seed 123 --output-dir output_seed123

# 較慢請求間隔；暫時網路錯誤最多額外重試 2 次，10、20 秒退避
.\.venv\Scripts\python.exe estimate.py --sample-size 500 --request-interval 3 --retries 2 --retry-backoff 10

# 若要依清理後實際 ID 數外推，可明確更改數字
.\.venv\Scripts\python.exe estimate.py --sample-size 500 --population-size 56093

.\.venv\Scripts\python.exe estimate.py --help
```

| 參數 | 預設 | 用途 |
|---|---|---|
| `--input` | 自動尋找 | 優先使用 `youtube_urls.txt` |
| `--output-dir` | 程式旁的 `output` | 結果與續跑進度 |
| `--sample-size` | 500 | 有效影片不足時使用全部 |
| `--seed` | 42 | 決定抽樣與 bootstrap 的種子 |
| `--workers` | 1 | 同時處理的影片數，1–4；CSV 仍由主執行緒依序寫入 |
| `--population-size` | 56093 | 整批容量外推的影片總數 |
| `--request-interval` | 2 | 全部工作共用的 HTTP 請求開始間隔秒數，至少 1 秒 |
| `--timeout` | 30 | 單次網路讀取逾時秒數 |
| `--retries` | 2 | 暫時錯誤額外重試次數，0–3 |
| `--retry-backoff` | 10 | 重試指數退避起始秒數 |
| `--bootstrap-iterations` | 10000 | bootstrap 次數，至少 100 |
| `--retry-failed` | 關閉 | 重試一般失敗，成功 metadata 永不重查 |
| `--report-only` | 關閉 | 只做離線統計 |

結束碼：0 為批次已完成或只產生報告（個別影片仍可能失敗）；1 為設定、檔案或非預期錯誤；2 為限流停止；130 為 Ctrl+C。

## 檔案結構

```text
youtube_size_estimator/
├── estimate.py
├── requirements.txt
├── README.md
├── tests/
│   └── test_estimate.py
├── .venv/
└── output/
    ├── sampled_urls.txt
    ├── sample_manifest.json
    ├── metadata_results.csv
    ├── failed_urls.csv
    ├── summary.json
    ├── report.md
    └── report_history/       # 再次產生報告時保留舊版報告
```

`metadata_results.csv` 保存要求的所有欄位，另含所選格式 ID、逐串流估算明細、metadata 警告、累積嘗試次數、UTC 時間與 yt-dlp 版本。缺失數值以空白儲存，`estimation_method=unknown`；JSON 使用 `null`，報告顯示 unknown。`video_bitrate`、`audio_bitrate` 單位為 kbps，`filesize`、`filesize_approx` 為 bytes，`estimated_total_size_mb` 為十進位 MB。

CSV 使用 UTF-8 BOM，方便 Windows Excel 開啟。標題與錯誤可能包含換行或逗號，請用標準 CSV reader 解析，不要單純以行數計算影片數。

兩個 CSV **僅附加、不覆蓋既有採集列**。一般失敗若重試，會新增一列；統計每個 ID 的最新狀態，並保留已成功結果。`failed_urls.csv` 是失敗歷史，已恢復的影片仍保留原始錯誤；目前失敗數以 `summary.json` 為準。成功取得 metadata 但容量 unknown 的影片不算 metadata 失敗，也不列入失敗清單，其原因保存在 metadata CSV 的 `error_message`。

`summary.json` 與 `report.md` 每次正常完成、限流或 Ctrl+C 後更新；舊版本先存入 `report_history/`。請保留整個 output 資料夾，特別是 manifest、抽樣名單及 metadata CSV。

每個輸出目錄只允許一個程序操作。`.run.lock` 在正常退出和 Ctrl+C 後自動移除。若電腦當機或強制終止留下鎖檔，確認沒有採集程序仍在執行後才刪除鎖檔。突然斷電造成 CSV 最後一筆不完整時，程式會拒絕忽略或覆蓋它；請先備份並檢查。

## 輸入清理與可重現抽樣

搜尋目前工作目錄、程式目錄及程式父目錄中的 TXT，不遞迴掃描 output 或虛擬環境。排除 requirements.txt 與 sampled_urls.txt；多檔案優先 youtube_urls.txt，其餘按完整路徑排序選第一個，並顯示選用檔案。建議有多個同名檔案時使用 `--input`。

接受 UTF-8、UTF-8 BOM 與有 BOM 的 UTF-16 TXT。移除每行前後空白，排除空行，嚴格驗證 YouTube 網域、URL 結構及 11 字元影片 ID。支援 watch、youtu.be、shorts、embed、live、v URL；忽略時間戳、播放清單等附加參數，以影片 ID 去重，統一輸出 watch URL。結構有效不表示影片未刪除、公開或所在區域可存取。

將所有唯一 ID 排序，再以 `random.Random(seed).shuffle()` 打亂並取前 N 個，為不放回均勻隨機抽樣。這樣 3 支與 500 支的前三支一致；同一母體、seed 與 Python 環境可重現。相同輸出目錄允許從 3 擴大至 500，既有抽樣順序不變。更改 seed、清理後 ID 集合或縮小樣本會被拒絕，需另用輸出目錄。空白／重複行改變而 ID 集合不變則可續跑。

不因失敗而補抽其他影片，以保留原本的隨機樣本。已取得成功 metadata 的 ID（包括容量 unknown）永不自動重查；如需重新研究，請明確建立新的輸出目錄。

## 格式與容量計算

固定使用 `bestvideo*+bestaudio/best`。以 yt-dlp 格式選擇器處理 `requested_formats`、`requested_downloads` 或原始 `formats`。解析度與 fps 優先排序；音訊使用 yt-dlp 的最佳音訊排序。不以音訊串流高度判斷影片畫質，也不把所有可用格式容量相加。

對**每個真正選中的串流**依序使用：

1. `filesize`：metadata 宣告的精確 bytes。
2. `filesize_approx`：近似 bytes。
3. bitrate × duration：`kbps × 1000 × 秒 / 8` 得到 bytes。
4. 缺少必要資訊：unknown。

分離音影片先各自估算再相加；單一影音混合串流只計一次。bitrate 優先使用該串流的 `tbr`；混合串流若沒有 `tbr`，必須同時具備 `vbr` 和 `abr`，不能只算其中一項。video-only 或 audio-only 才可單用對應 bitrate。任一所選串流未知，整支影片總容量即為 unknown，不以已知部分充當總量。

全為精確大小時，總量寫入 `filesize`。含近似而無 bitrate 時，總量寫入 `filesize_approx`。任一串流依賴 bitrate 時，分類為 bitrate，總量只寫入 `estimated_total_size_mb`，不冒充 filesize 或 filesize_approx。逐串流原始值與方法保存在 `stream_details_json`。直播中、尚未開始或直播仍在處理的完整容量一律 unknown。

程式固定 `extract_info(url, download=False)`，另設 `skip_download=True`、`simulate=True`、`check_formats=False`，關閉縮圖、字幕、描述、info JSON、留言與後處理輸出，並封鎖 yt-dlp 的媒體下載入口。只取得影片頁、API 回應、格式 manifest、播放器腳本等 metadata 所需資料，不抓取影片／音訊片段，不以實際下載驗證 filesize。無需 ffmpeg。

本機若已有 Node.js、Deno、Bun 或 QuickJS，會交由 yt-dlp 使用其標準播放器簽章處理；`yt-dlp[default]` 安裝對應官方套件。不自動安裝執行環境或啟用遠端元件下載。若沒有相容執行環境或 YouTube 限制格式，可能僅取得部分格式或失敗，警告保存在 CSV；不宣稱已驗證所有最高畫質均可下載。

## 統計定義與限制

- 全部容量統計使用**成功取得正值容量估計**的樣本；unknown 與失敗不當作 0。
- 平均時長主指標使用上述樣本中有時長者；另提供全部成功 metadata 的平均時長與各自分母。
- P90、P95 使用排序後的線性插值分位數；平均寬／高為算術平均。
- 解析度以短邊分為 720p、1080p、1440p、2160p 及其他，讓 1080×1920 的直式影片列為 1080p；寬度缺失時用高度。JSON 及報告亦顯示全部 metadata 成功樣本的各解析度計數。
- 有效 URL 比例 = 結構有效 URL 行／非空白行（含重複）；另列不重複有效 ID／非空白行。
- metadata 成功率與容量估計比例主要分母為抽樣數；另列已處理與 metadata 成功作分母的比例。未完成批次會明列待處理及缺失比例。
- 精確、近似、bitrate、unknown 的分類數以已處理 ID 計算，總和等於已處理數；失敗包含於 unknown。待處理數可能包含已寫入限流／中斷狀態、仍待續跑的 ID。
- 單位：1 MB = 10^6 bytes、1 GB = 10^9 bytes、1 TB = 10^12 bytes，不是 MiB／GiB／TiB。
- 整批 TB = 已知樣本平均 MB × 56,093 / 10^6。預留 20% 為乘以 1.20；指定總數與清理後 ID 數分別列出。
- 95% bootstrap 信賴區間：以已知容量樣本有放回抽樣 10,000 次，計算平均數的第 2.5 與 97.5 百分位，再線性換算成整批容量。seed 固定、分批計算節省記憶體，不做有限母體修正。少於 2 個已知容量不產生區間。

**信賴區間只反映抽樣誤差，不包含 bitrate 或 filesize_approx 的估算誤差；也不保證失敗／容量未知的影片和成功樣本分布一致。** 三支實測主要確認流程，小樣本不能提供穩健的大批量容量結論。另需考慮分布長尾、來源最高畫質存取限制、音影片合併容器與檔案系統開銷。這些不確定性不會因 bootstrap 自動消失。

## 相依套件與參考

安裝指令使用 `--upgrade`，取得符合 Python 版本的最新版相容套件；實際 yt-dlp 版本逐筆記錄。此次環境為 Python 3.12、yt-dlp 2026.10.26。可使用以下指令檢查及記錄自己的環境：

```powershell
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m pip freeze
```

實作參考 [yt-dlp 官方 Python API 範例](https://github.com/yt-dlp/yt-dlp#embedding-examples)、[格式選擇說明](https://github.com/yt-dlp/yt-dlp#format-selection)、[PyPI 相依套件與版本資訊](https://pypi.org/project/yt-dlp/)。
