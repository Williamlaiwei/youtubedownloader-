# 下載紀錄 – youtube camera_control 樣本

## 執行設定
- 來源清單:`youtube_urls.txt`(56,093 條),隨機抽樣 20 支,seed 42
- 工具:yt-dlp 2026.10.26,JS runtime:node,ffmpeg 合併
- 格式:`bv*+ba/b`,排序 `res`、`vcodec:av01`(不設畫質上限,同分辨率偏好 AV1),輸出 mkv
- 下載方式:循序(單線),未使用 cookies、代理
- 輸出位置:`SUPPLIER-v1-1028-youtube_camera_control\`(每個視頻一個目錄,含 `metadata.json`、`001.mkv`)

## 結果
- 成功 20/20,失敗 0
- 總大小 4.32 GB
- 平均大小 216.0 MB(中位數 159.3,標準差 178.9,範圍 18.6–755.5)
- 平均單支耗時 40.2 秒;平均速度 5.38 MB/s(合計大小 ÷ 合計耗時,含合併)
- 畫質:1080p 13 支、720p 6 支、2160p 1 支
- 質檢:ffprobe 實際分辨率與 metadata 20/20 一致;九個必填欄位中,1 支 `like_count` 為空(疑似隱藏點贊數)

## 外推(56,093 支,僅供參考)
- 總容量約 12.1 TB(95% 區間約 7.8–16.5 TB)
- 循序下載約 26 天(區間約 17.5–35 天);並發加速**尚未實測**
- 期限 2026-11-20

## 備註
- 耗時與速度來自第一次量測(下載後即刪除);交付目錄是第二次重新下載的同樣 20 支,檔案內容一致,但第二次未記錄耗時。
- 逐支明細見 `download_log.csv`;抽樣與耗時原始資料見 `estimate\sample20\results.json`。
- 樣例包(2 支)見 `SUPPLIER-youtube_camera_control需求样例\`。
