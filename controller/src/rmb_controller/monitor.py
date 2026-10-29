"""Visible local desktop monitor. Collection belongs to this window's lifetime."""
from datetime import datetime, timezone
from contextlib import closing
import json
import os
from pathlib import Path
import queue
import sqlite3
import threading
import time
import traceback
import tkinter as tk
from tkinter import ttk

from .artifacts import controller_lock
from .database import SCHEMA_VERSION
from .metadata import MAX_INTERVAL, MAX_WORKERS, MIN_INTERVAL, collect_metadata


def record_monitor_error(data_dir, exc, stage='collection'):
    """Keep the actual traceback and runtime identity, not just a transient label."""
    path = Path(data_dir) / 'logs/monitor_errors.jsonl'
    record = {'at_utc': datetime.now(timezone.utc).isoformat(), 'pid': os.getpid(),
              'error': str(exc), 'traceback': traceback.format_exc(), 'stage': stage,
              'supported_schema_version': SCHEMA_VERSION,
              'monitor_source': str(Path(__file__).resolve()),
              'database_path': str((Path(data_dir) / 'controller.sqlite3').resolve())}
    try:
        with closing(sqlite3.connect(Path(record['database_path']).as_uri() + '?mode=ro', uri=True)) as db:
            record['database_schema_version'] = db.execute('PRAGMA user_version').fetchone()[0]
    except Exception as error:
        record['database_diagnostic_error'] = str(error)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + '\n')
        handle.flush()
        os.fsync(handle.fileno())
    return path


def collection_notice(result, restored=False):
    if result['status'] == 'rate_limited':
        return ('YouTube HTTP 429 限流',
                ('上次採集遇到 YouTube HTTP 429。' if restored else '這次請求遇到 YouTube HTTP 429。') +
                '\n本輪已停止，採集進度已保存；這不是壞連結。\n可按「立即嘗試續跑」再試一次，若仍限流會再次停止。')
    return None


def error_notice(message, stage='collection'):
    if stage == 'publication':
        return ('VMtask 更新失敗', '採集結果已保存在 SQLite 與 metadata。\n'
                'VMtask 整理尚未完成，請勿派發本次任務包。\n'
                '若 CSV 正被 Excel 或預覽程式使用，請先關閉後再重試整理。\n' + message)
    if 'Unsupported database schema version' in message:
        return ('資料庫版本不相容', '程式與資料庫版本不相容，這次尚未開始採集。\n' + message)
    return ('採集發生錯誤', message)


def monitor_snapshot(data_dir):
    path = Path(data_dir) / "controller.sqlite3"
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")
        total = db.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        visited = db.execute("SELECT COUNT(DISTINCT video_id) FROM collection_attempts").fetchone()[0]
        raw = db.execute("SELECT COUNT(DISTINCT video_id) FROM collected_metadata").fetchone()[0]
        failed = db.execute("""SELECT COUNT(*) FROM videos WHERE metadata_attempt_count>0
            AND metadata_status IN ('restricted','permanent_failure','retry_wait','rate_limited','insufficient','interrupted','error')""").fetchone()[0]
        partial = db.execute("SELECT COUNT(*) FROM videos WHERE collected_metadata_id IS NOT NULL AND metadata_status='partial'").fetchone()[0]
        run = db.execute("SELECT * FROM collection_runs ORDER BY started_at_utc DESC LIMIT 1").fetchone()
        latest = [dict(x) for x in db.execute("""SELECT a.video_id,a.outcome,a.error_code,a.attempted_at_utc,
            v.original_title FROM collection_attempts a JOIN videos v USING(video_id) ORDER BY a.id DESC LIMIT 30""")]
        cooldown = db.execute("SELECT value FROM controller_settings WHERE key='metadata_cooldown_until'").fetchone()
        return {"total": total, "visited": visited, "raw": raw, "failed": failed,
                "partial": partial, "run": dict(run) if run else None, "latest": latest,
                "cooldown_until": cooldown[0] if cooldown else None}
    finally:
        db.close()


def cooldown_seconds(value, current=None):
    if not value:
        return 0
    until=datetime.fromisoformat(value.replace('Z','+00:00'))
    if until.tzinfo is None:
        raise ValueError('Cooldown timestamp must include timezone')
    return max(0,(until-(current or datetime.now(timezone.utc))).total_seconds())


class Monitor:
    def __init__(self, window, project_root, workers=3, interval=1):
        self.window, self.project_root = window, Path(project_root)
        self.data_dir = self.project_root / "controller/data"
        self.events = queue.Queue()
        self.thread = None
        self.closing = False
        self.stop_pending = False
        self.waiting_cooldown = False
        self.started = None
        self.start_count = 0
        self.last_run = None
        self.notified = set()
        window.title(f"RMB YouTube — 本機 Metadata 採集監視器 [SQLite v{SCHEMA_VERSION}]")
        window.geometry("1040x720")
        window.minsize(860, 600)
        window.protocol("WM_DELETE_WINDOW", self.close)
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TLabel", font=("Microsoft JhengHei UI", 11))
        style.configure("TButton", font=("Microsoft JhengHei UI", 11), padding=7)
        outer = ttk.Frame(window, padding=20)
        outer.pack(fill="both", expand=True)
        ttk.Label(outer, text="逐一讀取全部 YouTube 網頁的 metadata", font=("Microsoft JhengHei UI", 18, "bold")).pack(anchor="w")
        ttk.Label(outer, text="網頁輕量 metadata · 不使用模型 · 不下載影片 · 解析度交由 VM 下載及 ffprobe 確認").pack(anchor="w", pady=(5, 15))
        self.status = tk.StringVar(value="準備就緒；按「開始／續跑」即可採集。")
        ttk.Label(outer, textvariable=self.status, wraplength=980).pack(anchor="w", pady=(0, 10))
        self.progress = ttk.Progressbar(outer, maximum=100, mode="determinate")
        self.progress.pack(fill="x")
        self.counts = tk.StringVar(value="正在讀取 SQLite…")
        ttk.Label(outer, textvariable=self.counts, font=("Microsoft JhengHei UI", 12, "bold")).pack(anchor="w", pady=10)
        self.details = tk.StringVar()
        ttk.Label(outer, textvariable=self.details).pack(anchor="w")
        controls = ttk.Frame(outer)
        controls.pack(fill="x", pady=12)
        self.start_button = ttk.Button(controls, text="開始／續跑全部清單", command=self.start)
        self.start_button.pack(side="left")
        self.stop_button = ttk.Button(controls, text="停止並保存", command=self.stop, state="disabled")
        self.stop_button.pack(side="left", padx=8)
        ttk.Button(controls, text="開啟任務資料夾", command=self.open_catalog).pack(side="left", padx=4)
        ttk.Label(controls, text="並行數").pack(side="left", padx=(18, 3))
        self.workers = tk.IntVar(value=workers)
        self.worker_input = ttk.Spinbox(controls, from_=1, to=4, width=3, textvariable=self.workers)
        self.worker_input.pack(side="left")
        self.workers.trace_add("write", self.apply_workers)
        ttk.Label(controls, text="共用請求間隔（秒）").pack(side="left", padx=(12, 3))
        self.interval = tk.DoubleVar(value=interval)
        self.interval_input = ttk.Spinbox(controls, from_=MIN_INTERVAL, to=MAX_INTERVAL, increment=1, width=4, textvariable=self.interval)
        self.interval_input.pack(side="left")
        self.interval.trace_add("write", self.apply_interval)
        ttk.Label(outer, text="最近完成的網頁（暫時錯誤會保留，不刪除 URL）").pack(anchor="w", pady=(8, 5))
        self.table = ttk.Treeview(outer, columns=("id", "status", "error", "title"), show="headings", height=12)
        for column, label, width in (("id", "video_id", 130), ("status", "狀態", 120), ("error", "分類", 160), ("title", "標題", 520)):
            self.table.heading(column, text=label)
            self.table.column(column, width=width, stretch=column == "title")
        self.table.pack(fill="both", expand=True)
        self.table.bind("<Double-1>", self.open_selected_folder)
        self.message = tk.StringVar(value="關閉此視窗會停止本次採集；重新開啟後可以續跑。")
        ttk.Label(outer, textvariable=self.message, wraplength=980).pack(anchor="w", pady=(12, 0))
        self.directory = tk.StringVar(value="任務包：" + str(self.project_root / "VMtask") + "（停止／完成採集後更新）")
        ttk.Label(outer, textvariable=self.directory, font=("Consolas", 9)).pack(anchor="w", pady=(4, 0))
        window.after(100, self.refresh)
        window.after(250, self.notify_pending_export)
        window.lift()
        window.after(200, window.focus_force)

    def open_catalog(self):
        target = self.project_root / "VMtask"
        os.startfile(target if (target / "READY.json").is_file() else self.data_dir / "catalog")

    def notify(self, key, title, message):
        """Show one modeless notice per event; collection/export can still finish saving."""
        if key in self.notified or self.closing:
            return
        self.notified.add(key)
        popup = tk.Toplevel(self.window)
        popup.title(title)
        popup.transient(self.window)
        popup.resizable(False, False)
        body = ttk.Frame(popup, padding=20)
        body.pack(fill='both', expand=True)
        ttk.Label(body, text=message, wraplength=650, justify='left').pack(anchor='w')
        ttk.Button(body, text='知道了', command=popup.destroy).pack(anchor='e', pady=(16, 0))
        popup.lift()

    def notify_collection(self, result, restored=False):
        notice = collection_notice(result, restored)
        if notice:
            self.notify(('collection', result['run_id']), *notice)

    def notify_pending_export(self):
        pending = self.data_dir / 'vmtask_archive/pending_publication.json'
        if pending.is_file():
            self.message.set('採集進度已保存；上次 VMtask 整理未完成，請關閉占用統計 CSV 的程式後重試整理。')
            self.notify(('publication_pending', str(pending)),
                        *error_notice('上次整理未完成；完整錯誤請見 controller/data/logs/monitor_errors.jsonl。', 'publication'))

    def open_selected_folder(self, event=None):
        selected = self.table.selection()
        if not selected:
            return
        video_id = self.table.item(selected[0], "values")[0]
        db = sqlite3.connect(self.data_dir / "controller.sqlite3")
        try:
            row = db.execute("SELECT folder_name,assigned_part FROM videos WHERE video_id=?", (video_id,)).fetchone()
        finally:
            db.close()
        if row and row[0]:
            destination = self.project_root / "VMtask" / f"batch-{row[1]:02d}" / "videos" / row[0] if row[1] else None
            os.startfile(destination if destination and destination.is_dir() else self.data_dir / "catalog" / row[0])

    def start(self, manual=True):
        if self.thread and self.thread.is_alive():
            return
        try:
            workers, interval = self.workers.get(), self.interval.get()
            if workers not in range(1, MAX_WORKERS + 1) or not MIN_INTERVAL <= interval <= MAX_INTERVAL:
                raise ValueError("並行數須為 1–4；請求間隔須為 1–30 秒。")
            snapshot = monitor_snapshot(self.data_dir)
            self.start_count = snapshot['visited']
            if cooldown_seconds(snapshot['cooldown_until']) > 0 and not manual:
                self.waiting_cooldown = True
                self.start_button.configure(state='normal', text='立即嘗試續跑')
                self.stop_button.configure(state='normal')
                self.status.set('YouTube 限流冷卻中，進度已保存。')
                reason = '上次遇到 HTTP 429；' if (snapshot.get('run') or {}).get('status') == 'rate_limited' else '上次採集已停止；'
                self.message.set(reason + '可按「立即嘗試續跑」現在測試。等待期間也會安排自動續跑，停止可取消。')
                if snapshot.get('run'):
                    self.notify_collection(snapshot['run'], restored=True)
                self.notify_pending_export()
                return
        except Exception as exc:
            self.status.set(str(exc))
            try:
                self.message.set('錯誤紀錄：' + str(record_monitor_error(self.data_dir, exc)))
            except OSError:
                pass
            self.notify(('start_error', str(exc)), *error_notice(str(exc)))
            return
        self.started = time.monotonic()
        self.waiting_cooldown = False
        self.stop_pending = False
        self.status.set("採集中：逐一讀取清單中的 YouTube 網頁…")
        self.start_button.configure(state="disabled", text='採集中')
        self.stop_button.configure(state="normal")
        def collect():
            try:
                with controller_lock(self.data_dir):
                    result = collect_metadata(self.project_root, self.data_dir, workers=workers, interval=interval,
                        all_pages=True, manual_resume=manual,
                        progress=lambda text, **kwargs: self.events.put(("progress", text)))
                # Report the saved collection outcome before export can fail or take time.
                self.events.put(('collection_saved', dict(result)))
                if (self.project_root / "VMtask/manifest.json").is_file():
                    self.events.put(("progress", "採集已保存，正在更新 VMtask 的 20 個 batch 與錯誤紀錄…"))
                    try:
                        from .vmtask import export_vmtask
                        published = export_vmtask(self.project_root)
                        result['publish_status'] = 'completed'
                        result["message"] = f"VMtask 已合併到固定 20 batch：{published['ready_count']} 個可派發、{published['held_count']} 個暫留。"
                    except Exception as exc:
                        try:
                            record_monitor_error(self.data_dir, exc, stage='publication')
                        except OSError:
                            pass
                        result['publish_status'] = 'failed'
                        result['publish_error'] = str(exc)
                        result['message'] = '採集進度已保存；VMtask 整理失敗，暫不可派發。' + str(exc)
                self.events.put(("done", result))
            except BaseException as exc:
                try:
                    record_monitor_error(self.data_dir, exc)
                except OSError:
                    pass
                self.events.put(("error", str(exc)))
        # Non-daemon: the window explicitly requests stop and waits for durable writes.
        self.thread = threading.Thread(target=collect, name="visible-metadata-collector")
        self.thread.start()

    def stop(self):
        if self.waiting_cooldown and (self.thread is None or not self.thread.is_alive()):
            self.waiting_cooldown = False
            self.start_button.configure(state='normal', text='開始／續跑全部清單')
            self.stop_button.configure(state='disabled')
            self.status.set('已暫停；冷卻後不會自動續跑，按開始可重新安排。')
            return
        self.stop_pending = True
        self.request_stop()
        self.status.set("正在停止：等待已送出的網頁請求返回並保存，請勿強制結束。")
        self.start_button.configure(state='disabled', text='正在保存…')
        self.stop_button.configure(state="disabled")

    def save_setting(self, key, value, done_message, failed_label):
        """Hand a live setting to the running collector through SQLite; outside a run values are read at start."""
        try:
            db = sqlite3.connect(self.data_dir / "controller.sqlite3", timeout=5)
            try:
                db.execute("INSERT OR REPLACE INTO controller_settings VALUES(?,?)", (key, str(value)))
                db.commit()
            finally:
                db.close()
            self.message.set(done_message)
        except Exception as exc:
            self.message.set(f"{failed_label}失敗：{exc}")

    def apply_workers(self, *_):
        if not (self.thread and self.thread.is_alive()):
            return
        try:
            value = int(self.workers.get())
        except (tk.TclError, TypeError, ValueError):
            return
        if value in range(1, MAX_WORKERS + 1):
            self.save_setting('desired_workers', value, f"並行數已調整為 {value}；進行中的請求完成後生效。", "調整並行數")

    def apply_interval(self, *_):
        if not (self.thread and self.thread.is_alive()):
            return
        try:
            value = float(self.interval.get())
        except (tk.TclError, TypeError, ValueError):
            return
        if MIN_INTERVAL <= value <= MAX_INTERVAL:  # NaN fails this comparison too
            self.save_setting('desired_interval', value, f"請求間隔已調整為 {value:g} 秒；已排定的下一次請求最久等舊間隔。", "調整請求間隔")

    def request_stop(self):
        try:
            db = sqlite3.connect(self.data_dir / "controller.sqlite3", timeout=5)
            try:
                db.execute("INSERT OR REPLACE INTO controller_settings VALUES('stop_requested','1')")
                db.commit()
            finally:
                db.close()
        except Exception as exc:
            self.message.set(f"停止請求失敗：{exc}")

    def close(self):
        if self.thread and self.thread.is_alive():
            self.closing = True
            self.stop()
        else:
            self.window.destroy()

    def refresh(self):
        try:
            while True:
                event, value = self.events.get_nowait()
                if event == "progress":
                    self.message.set(value)
                elif event == 'collection_saved':
                    if value['status'] == 'rate_limited':
                        self.status.set('YouTube HTTP 429：已停止並保存進度，正在整理 VMtask。')
                    self.notify_collection(value)
                elif event in {"done", "error"}:
                    requested_stop = self.stop_pending or self.closing
                    self.stop_pending = False
                    self.start_button.configure(state="normal", text='開始／續跑全部清單')
                    self.stop_button.configure(state="disabled")
                    self.worker_input.configure(state="normal")
                    self.interval_input.configure(state="normal")
                    if event == "error":
                        notice = error_notice(value)
                        self.status.set(notice[0] + '：' + value)
                        self.message.set('完整錯誤紀錄：' + str(self.data_dir / 'logs/monitor_errors.jsonl'))
                        self.notify(('collection_error', value), *notice)
                    else:
                        labels = {"completed": "本輪完成", "interrupted": "已停止並保存", "rate_limited": "限流停止，請稍後續跑", "network_blocked": "網路／存取異常，已保存並停止", "error": "錯誤停止"}
                        self.status.set(labels.get(value["status"], value["status"]))
                        self.message.set(value.get("message") or "資料已保存；未完成的 URL 仍保留在 SQLite。")
                        self.notify_collection(value)
                        if value.get('publish_status') == 'failed':
                            self.notify(('publication', value['run_id']), *error_notice(value['publish_error'], 'publication'))
                        if value['status'] in {'rate_limited','network_blocked'} and not requested_stop:
                            self.waiting_cooldown = True
                            self.start_button.configure(state='normal', text='立即嘗試續跑')
                            self.stop_button.configure(state='normal')
                            if value.get('publish_status') != 'failed':
                                self.message.set('採集進度已保存。可按「立即嘗試續跑」現在測試；等待期間也會安排自動續跑，停止可取消。')
        except queue.Empty:
            pass
        if self.stop_pending and self.thread and self.thread.is_alive():
            self.request_stop()
        try:
            data = monitor_snapshot(self.data_dir)
            total = data["total"]
            self.progress["value"] = 100 * data["visited"] / total if total else 0
            self.counts.set(f"本次新流程已查 {data['visited']:,} / {total:,}　｜　已存原始 metadata {data['raw']:,}　｜　限制／錯誤 {data['failed']:,}　｜　缺欄位 {data['partial']:,}")
            remaining = max(0, total - data["visited"])
            detail = f"尚未查詢 {remaining:,} 個網頁"
            if self.waiting_cooldown:
                seconds = cooldown_seconds(data['cooldown_until'])
                if seconds > 0:
                    minutes, sec = divmod(int(seconds),60);hours,minutes=divmod(minutes,60)
                    reason = 'HTTP 429 限流；' if (data.get('run') or {}).get('status') == 'rate_limited' else ''
                    self.status.set(f'{reason}自動續跑倒數 {hours:02d}:{minutes:02d}:{sec:02d}；可立即嘗試續跑。')
                    detail += '　｜　冷卻至 '+str(data['cooldown_until'])+' UTC'
                else:
                    self.waiting_cooldown = False
                    self.window.after(0,lambda: self.start(manual=False))
            if self.started and self.thread and self.thread.is_alive():
                elapsed = time.monotonic() - self.started
                delta = data["visited"] - self.start_count
                if delta > 0 and elapsed > 10:
                    per_hour = delta / elapsed * 3600
                    detail += f"　｜　目前約 {per_hour:,.0f} 頁／小時　｜　剩餘約 {remaining / per_hour:.1f} 小時（動態估計）"
            if data["run"]:
                detail += f"　｜　本輪 {data['run']['processed_count']:,}/{data['run']['selected_count']:,}"
            self.details.set(detail)
            self.table.delete(*self.table.get_children())
            for item in data["latest"]:
                self.table.insert("", "end", values=(item["video_id"], item["outcome"], item["error_code"] or "", item["original_title"] or ""))
        except Exception as exc:
            self.message.set("讀取狀態：" + str(exc))
        if self.closing and (self.thread is None or not self.thread.is_alive()):
            self.window.destroy()
            return
        self.window.after(1000, self.refresh)


def run_monitor(project_root, workers=3, interval=1):
    window = tk.Tk()
    monitor = Monitor(window, project_root, workers, interval)
    window.after(500, lambda: monitor.start(manual=False))
    window.mainloop()
    return 0
