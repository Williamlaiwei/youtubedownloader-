from contextlib import closing
from datetime import datetime,timezone
import json
import queue
import socket
import sqlite3
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest

import test_offline_import as offline
from rmb_controller.database import connect_database
from rmb_controller.ingest import import_existing, utc_now
from rmb_controller.metadata import Collected, collect_metadata, persist_collected, selected_videos
from rmb_controller.monitor import (Monitor, collection_notice, cooldown_seconds, error_notice,
                                    monitor_snapshot, record_monitor_error)
from rmb_controller.youtube_adapter import YouTubeAdapter


class FakeAdapter:
    def extract(self, video_id):
        return {"raw": {"id": video_id, "title": "Collected title", "description": "Actual description",
                        "uploader": "Author", "uploader_url": "https://www.youtube.com/@author",
                        "duration": 30, "view_count": 0, "like_count": 0},
                "summary": {"resolution": "1920x1080", "width": 1920, "height": 1080,
                            "selected_format_ids": "v+a", "filesize": 1234, "estimation_method": "exact"},
                "warnings": []}

    def stop(self):
        pass


class MonitorCollectionTests(unittest.TestCase):
    setUp = offline.OfflineImportTests.setUp
    query = offline.OfflineImportTests.query

    def test_cooldown_countdown_does_not_allow_early_requests(self):
        current=datetime(2026,10,28,10,0,tzinfo=timezone.utc)
        self.assertEqual(cooldown_seconds('2026-10-28T12:00:00+00:00',current),7200)
        self.assertEqual(cooldown_seconds('2026-10-28T09:00:00+00:00',current),0)
        with self.assertRaises(ValueError):cooldown_seconds('2026-10-28T12:00:00',current)

    def test_429_schema_and_publication_have_separate_notices(self):
        title,message=collection_notice({'status':'rate_limited'})
        self.assertIn('HTTP 429',title)
        self.assertIn('進度已保存',message)
        self.assertIn('上次',collection_notice({'status':'rate_limited'},restored=True)[1])
        self.assertIsNone(collection_notice({'status':'completed'}))
        schema=error_notice('Unsupported database schema version: 4 (supported: 3)')
        publication=error_notice('[WinError 5] bad_files_current.csv','publication')
        self.assertEqual(schema[0],'資料庫版本不相容')
        self.assertEqual(publication[0],'VMtask 更新失敗')
        self.assertIn('尚未完成',publication[1])

    def test_one_popup_per_collection_run_without_blocking_the_monitor(self):
        monitor=object.__new__(Monitor)
        monitor.notified=set();monitor.closing=False;monitor.window=object()
        with patch('rmb_controller.monitor.tk.Toplevel') as popup, \
             patch('rmb_controller.monitor.ttk.Frame'), \
             patch('rmb_controller.monitor.ttk.Label'), \
             patch('rmb_controller.monitor.ttk.Button'):
            monitor.notify_collection({'status':'rate_limited','run_id':'one'})
            monitor.notify_collection({'status':'rate_limited','run_id':'one'})
            popup.assert_called_once()
            popup.return_value.grab_set.assert_not_called()
            monitor.notify_collection({'status':'rate_limited','run_id':'two'})
            self.assertEqual(popup.call_count,2)

    def test_failed_export_cannot_hide_429_or_saved_collection(self):
        import_existing(self.root)
        vm=self.root/'VMtask';vm.mkdir();(vm/'manifest.json').write_text('{}',encoding='utf-8')
        monitor=object.__new__(Monitor)
        monitor.thread=None;monitor.data_dir=self.data;monitor.project_root=self.root
        monitor.workers=Mock();monitor.workers.get.return_value=3
        monitor.interval=Mock();monitor.interval.get.return_value=1
        monitor.status=Mock();monitor.message=Mock();monitor.start_button=Mock();monitor.stop_button=Mock()
        monitor.worker_input=Mock();monitor.interval_input=Mock();monitor.events=queue.Queue()
        result={'status':'rate_limited','run_id':'429-test','message':'Rate limit detected'}
        with patch('rmb_controller.monitor.collect_metadata',return_value=result), \
             patch('rmb_controller.monitor.threading.Thread') as thread, \
             patch('rmb_controller.vmtask.export_vmtask',side_effect=PermissionError('Access denied: bad_files_current.csv')):
            monitor.start()
            thread.call_args.kwargs['target']()
        events=[]
        while not monitor.events.empty():events.append(monitor.events.get_nowait())
        self.assertEqual(events[0],('collection_saved',{'status':'rate_limited','run_id':'429-test','message':'Rate limit detected'}))
        self.assertEqual(events[-1][0],'done')
        self.assertEqual(events[-1][1]['status'],'rate_limited')
        self.assertEqual(events[-1][1]['publish_status'],'failed')
        error=json.loads((self.data/'logs/monitor_errors.jsonl').read_text(encoding='utf-8'))
        self.assertEqual(error['stage'],'publication')

    def test_unsupported_schema_is_logged_with_expected_version_and_database_path(self):
        import_existing(self.root)
        with closing(connect_database(self.data/'controller.sqlite3')) as db, db:
            db.execute('PRAGMA user_version=4')
        try:
            connect_database(self.data/'controller.sqlite3')
        except ValueError as exc:
            self.assertIn('supported: 3',str(exc))
            path=record_monitor_error(self.data,exc)
        else:
            self.fail('Unknown schema must remain protected')
        error=json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual(error['supported_schema_version'],3)
        self.assertEqual(error['database_schema_version'],4)
        self.assertIn('Unsupported database schema version',error['traceback'])
        self.assertTrue(error['database_path'].endswith('controller.sqlite3'))

    def test_schema_three_database_can_resume_without_reinitialization(self):
        import_existing(self.root)
        with closing(connect_database(self.data/'controller.sqlite3')) as db:
            self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0],3)
        first=collect_metadata(self.root,self.data,limit=1,adapter=FakeAdapter(),
                               manual_resume=True,progress=lambda *a,**kw:None)
        second=collect_metadata(self.root,self.data,limit=1,adapter=FakeAdapter(),
                                manual_resume=True,progress=lambda *a,**kw:None)
        self.assertEqual((first['status'],second['status']),('completed','completed'))
        self.assertEqual(len(self.query('SELECT * FROM collected_metadata')),2)
        self.assertEqual(len(self.query('SELECT * FROM videos')),5)

    def test_resume_button_gives_feedback_during_cooldown_and_stop_cancels_schedule(self):
        class Control:
            def __init__(self,value=None):self.value=value;self.options={}
            def get(self):return self.value
            def set(self,value):self.value=value
            def configure(self,**kwargs):self.options.update(kwargs)
        monitor=object.__new__(Monitor)
        monitor.thread=None;monitor.waiting_cooldown=False;monitor.stop_pending=False
        monitor.data_dir=self.data;monitor.project_root=self.root
        monitor.workers=Control(3);monitor.interval=Control(1)
        monitor.worker_input=Control();monitor.interval_input=Control();monitor.events=queue.Queue()
        monitor.start_button=Control();monitor.stop_button=Control()
        monitor.status=Control();monitor.message=Control()
        snapshot={'visited':100,'cooldown_until':'2099-01-01T00:00:00+00:00',
                  'run':{'status':'rate_limited','run_id':'previous'}}
        with patch('rmb_controller.monitor.monitor_snapshot',return_value=snapshot), \
             patch('rmb_controller.monitor.Monitor.notify'), \
             patch('rmb_controller.monitor.threading.Thread') as thread, \
             patch('rmb_controller.monitor.collect_metadata') as collect:
            monitor.start(manual=False)
            monitor.start(manual=False)
            self.assertTrue(monitor.waiting_cooldown)
            self.assertEqual(monitor.start_button.options['state'],'normal')
            self.assertEqual(monitor.start_button.options['text'],'立即嘗試續跑')
            self.assertIn('429',monitor.message.value)
            monitor.stop()
            self.assertFalse(monitor.waiting_cooldown)
            self.assertEqual(monitor.start_button.options['text'],'開始／續跑全部清單')
            monitor.start(manual=False)
            self.assertTrue(monitor.waiting_cooldown)
            thread.assert_not_called();collect.assert_not_called()
            monitor.start()  # A deliberate click overrides the automatic waiting period.
            self.assertFalse(monitor.waiting_cooldown)
            thread.assert_called_once()
            thread.call_args.kwargs['target']()
            self.assertTrue(collect.call_args.kwargs['manual_resume'])
            monitor.start()
            thread.assert_called_once()  # Repeated clicks cannot start another live collector.

    def test_manual_resume_overrides_cooldown_and_keeps_audit(self):
        import_existing(self.root)
        until='2099-01-01T00:00:00+00:00'
        with closing(connect_database(self.data / 'controller.sqlite3')) as db, db:
            db.execute("INSERT INTO controller_settings VALUES('metadata_cooldown_until',?)",(until,))
        adapter=FakeAdapter()
        with patch.object(adapter,'extract',wraps=adapter.extract) as extract:
            with self.assertRaisesRegex(ValueError,'cooling down'):
                collect_metadata(self.root,self.data,limit=1,adapter=adapter)
            extract.assert_not_called()
            result=collect_metadata(self.root,self.data,limit=1,adapter=adapter,
                                    manual_resume=True,progress=lambda *a,**kw:None)
        self.assertEqual(result['status'],'completed')
        self.assertEqual(len(self.query('SELECT * FROM collected_metadata')),1)
        self.assertEqual(self.query("SELECT * FROM controller_settings WHERE key='metadata_cooldown_until'"),[])
        run=self.query('SELECT * FROM collection_runs')[0]
        options=json.loads(run['settings_json'])
        self.assertTrue(options['manual_resume'])
        self.assertEqual(options['previous_cooldown_until'],until)

    def test_manual_resume_stops_after_one_probe_if_still_rate_limited(self):
        import_existing(self.root)
        with closing(connect_database(self.data / 'controller.sqlite3')) as db, db:
            db.execute("INSERT INTO controller_settings VALUES('metadata_cooldown_until','2099-01-01T00:00:00+00:00')")
        adapter=FakeAdapter()
        with patch.object(adapter,'extract',side_effect=RuntimeError('HTTP Error 429: Too Many Requests')) as extract:
            result=collect_metadata(self.root,self.data,workers=3,adapter=adapter,
                                    manual_resume=True,progress=lambda *a,**kw:None)
        self.assertEqual(result['status'],'rate_limited')
        extract.assert_called_once()
        self.assertEqual(len(self.query('SELECT * FROM videos')),5)
        self.assertEqual(len(self.query('SELECT * FROM collection_attempts')),1)
        self.assertEqual(len(self.query("SELECT * FROM controller_settings WHERE key='metadata_cooldown_until'")),1)

    def _active_counts_with_live_change(self,start_workers,new_workers):
        """Active requests seen at the start of each call, after desired_workers changes during call 1."""
        import_existing(self.root)
        path=self.data/'controller.sqlite3'
        class Tracking(FakeAdapter):
            def __init__(self):
                self.lock=threading.Lock();self.active=0;self.calls=0;self.seen=[]
            def extract(self,video_id):
                with self.lock:
                    self.active+=1;self.calls+=1;first=self.calls==1;self.seen.append(self.active)
                if first:
                    with closing(sqlite3.connect(path,timeout=5)) as db,db:
                        db.execute("INSERT OR REPLACE INTO controller_settings VALUES('desired_workers',?)",(str(new_workers),))
                time.sleep(.2)
                with self.lock:self.active-=1
                return FakeAdapter.extract(self,video_id)
        adapter=Tracking()
        result=collect_metadata(self.root,self.data,workers=start_workers,all_pages=True,adapter=adapter,
                                progress=lambda *a,**kw:None)
        self.assertEqual(result['status'],'completed')
        return adapter.seen

    def test_worker_count_can_be_lowered_while_collecting(self):
        seen=self._active_counts_with_live_change(3,1)
        self.assertEqual(len(seen),5)
        self.assertEqual(seen[3:],[1,1])  # in-flight requests drain; no new ones overlap

    def test_worker_count_can_be_raised_while_collecting(self):
        seen=self._active_counts_with_live_change(1,3)
        self.assertGreaterEqual(max(seen),2)

    def test_out_of_range_desired_workers_is_clamped(self):
        seen=self._active_counts_with_live_change(1,99)
        self.assertLessEqual(max(seen),4)

    def test_monitor_applies_worker_change_only_while_collecting(self):
        class Control:
            def __init__(self,value=None):self.value=value
            def get(self):return self.value
            def set(self,value):self.value=value
        import_existing(self.root)
        monitor=object.__new__(Monitor)
        monitor.data_dir=self.data;monitor.workers=Control(2);monitor.message=Control()
        monitor.thread=None
        monitor.apply_workers()
        self.assertEqual(self.query("SELECT * FROM controller_settings WHERE key='desired_workers'"),[])
        monitor.thread=Mock();monitor.thread.is_alive.return_value=True
        monitor.apply_workers()
        self.assertEqual(self.query("SELECT value FROM controller_settings WHERE key='desired_workers'")[0]['value'],'2')
        monitor.workers.value=9  # out of range is ignored, last valid value stays
        monitor.apply_workers()
        self.assertEqual(self.query("SELECT value FROM controller_settings WHERE key='desired_workers'")[0]['value'],'2')

    def _intervals_applied(self,new_interval,adapter_has_setter=True):
        import_existing(self.root)
        path=self.data/'controller.sqlite3'
        class Recording(FakeAdapter):
            def __init__(self):self.intervals=[];self.calls=0
            def extract(self,video_id):
                self.calls+=1
                if self.calls==1:
                    with closing(sqlite3.connect(path,timeout=5)) as db,db:
                        db.execute("INSERT OR REPLACE INTO controller_settings VALUES('desired_interval',?)",(str(new_interval),))
                time.sleep(1.2)  # outlive one 1s loop tick so the new value is read mid-run
                return FakeAdapter.extract(self,video_id)
        if adapter_has_setter:
            Recording.set_interval=lambda self,value:self.intervals.append(value)
        adapter=Recording()
        result=collect_metadata(self.root,self.data,workers=1,limit=2,all_pages=True,interval=2,
                                adapter=adapter,progress=lambda *a,**kw:None)
        self.assertEqual(result['status'],'completed')
        return adapter.intervals

    def test_request_interval_can_be_changed_while_collecting(self):
        self.assertEqual(self._intervals_applied(7),[7.0])

    def test_out_of_range_request_interval_is_clamped(self):
        self.assertEqual(self._intervals_applied(0.2),[1.0])
        self.assertEqual(self._intervals_applied(999),[30.0])

    def test_unparseable_or_non_finite_interval_is_ignored(self):
        self.assertEqual(self._intervals_applied('abc'),[])
        self.assertEqual(self._intervals_applied('nan'),[])

    def test_adapter_without_interval_setter_still_runs(self):
        self.assertEqual(self._intervals_applied(7,adapter_has_setter=False),[])

    def test_adapter_set_interval_updates_shared_pacer_immediately(self):
        pacer=SimpleNamespace(interval=30.0,next_request=time.monotonic()+30)
        adapter=object.__new__(YouTubeAdapter)
        adapter.pacer=pacer;adapter.interval=30.0
        adapter.set_interval(3)
        self.assertEqual((adapter.interval,pacer.interval),(3.0,3.0))
        self.assertLess(pacer.next_request-time.monotonic(),3.1)  # no waiting out the old 30s gate
        with self.assertRaises(ValueError):adapter.set_interval(0.5)

    def test_monitor_applies_interval_change_only_while_collecting(self):
        class Control:
            def __init__(self,value=None):self.value=value
            def get(self):return self.value
            def set(self,value):self.value=value
        import_existing(self.root)
        monitor=object.__new__(Monitor)
        monitor.data_dir=self.data;monitor.interval=Control(5.0);monitor.message=Control()
        monitor.thread=None
        monitor.apply_interval()
        self.assertEqual(self.query("SELECT * FROM controller_settings WHERE key='desired_interval'"),[])
        monitor.thread=Mock();monitor.thread.is_alive.return_value=True
        monitor.apply_interval()
        self.assertEqual(self.query("SELECT value FROM controller_settings WHERE key='desired_interval'")[0]['value'],'5.0')
        for bad in (0.5,31,float('inf'),float('nan')):
            monitor.interval.value=bad;monitor.apply_interval()
        self.assertEqual(self.query("SELECT value FROM controller_settings WHERE key='desired_interval'")[0]['value'],'5.0')

    def test_monitor_counts_actual_new_page_attempts_not_old_csv_rows(self):
        import_existing(self.root)
        before = monitor_snapshot(self.data)
        self.assertEqual((before["total"], before["visited"], before["raw"]), (5, 0, 0))
        with patch.object(socket, "socket", side_effect=AssertionError("Network prohibited in test")):
            result = collect_metadata(self.root, self.data, limit=1, workers=1, all_pages=True,
                                      adapter=FakeAdapter(), progress=lambda *args, **kwargs: None)
        self.assertEqual(result["status"], "completed")
        after = monitor_snapshot(self.data)
        self.assertEqual((after["visited"], after["raw"]), (1, 1))
        self.assertEqual(after["run"]["processed_count"], 1)

    def test_all_pages_includes_legacy_complete_samples_without_raw_extraction(self):
        import_existing(self.root)
        with closing(connect_database(self.data / "controller.sqlite3", readonly=True)) as db:
            self.assertEqual(len(selected_videos(db, all_pages=True)), 5)
            self.assertEqual(len(selected_videos(db)), 2)

    def test_new_success_is_skipped_when_resuming_the_whole_list(self):
        import_existing(self.root)
        collect_metadata(self.root, self.data, limit=1, workers=1, all_pages=True,
                         adapter=FakeAdapter(), progress=lambda *args, **kwargs: None)
        completed = self.query("SELECT video_id FROM collected_metadata")[0]["video_id"]
        with closing(connect_database(self.data / "controller.sqlite3", readonly=True)) as db:
            self.assertNotIn(completed, selected_videos(db, all_pages=True))

    def test_title_metadata_exists_immediately_after_one_result_is_saved(self):
        import_existing(self.root)
        with closing(connect_database(self.data / "controller.sqlite3")) as db:
            now = utc_now()
            with db:
                db.execute("INSERT INTO collection_runs VALUES('test-run',?,?,NULL,'running',1,0,'{}',NULL)", (now, now))
            video_id = offline.E  # This pending video has no title/folder yet.
            outcome = Collected(video_id, [{"at": now, "outcome": "success", "code": None,
                "message": None, "retryable": False}], FakeAdapter().extract(video_id))
            persist_collected(db, self.data, "test-run", outcome)
            video = db.execute("SELECT * FROM videos WHERE video_id=?", (video_id,)).fetchone()
            self.assertEqual(video["folder_name"], "Collected title")
            path = self.data / "catalog" / video["folder_name"] / "metadata.json"
            self.assertTrue(path.is_file())
            # No batch-finalization routine has run in this test.


if __name__ == "__main__":
    unittest.main()
