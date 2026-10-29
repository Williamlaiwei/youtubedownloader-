from contextlib import closing
import json
from pathlib import Path
import types
import unittest
from unittest.mock import patch

import test_offline_import as offline
from test_monitor_collection import FakeAdapter
from rmb_controller.database import connect_database
from rmb_controller.ingest import import_existing, utc_now
from rmb_controller.metadata import Collected, persist_collected
from rmb_controller.vmtask import export_vmtask, validate_vmtask
from rmb_controller.youtube_adapter import YouTubeAdapter


class BasicModeTests(unittest.TestCase):
    setUp = offline.OfflineImportTests.setUp
    query = offline.OfflineImportTests.query

    def save_basic(self, video_id=offline.E, missing_like=False):
        import_existing(self.root)
        result = FakeAdapter().extract(video_id)
        result.update(summary={}, metadata_mode='basic')
        if missing_like:
            result['raw']['like_count'] = None
        with closing(connect_database(self.data/'controller.sqlite3')) as db:
            now = utc_now()
            with db:
                db.execute("INSERT INTO collection_runs VALUES('basic-run',?,?,NULL,'running',1,0,'{}',NULL)", (now, now))
            outcome = Collected(video_id, [{'at':now,'outcome':'success','code':None,'message':None,'retryable':False}], result)
            persist_collected(db, self.data, 'basic-run', outcome)
            with db:
                db.execute("UPDATE collection_runs SET status='completed'")

    def test_basic_metadata_is_ready_and_exportable_without_resolution(self):
        self.save_basic()
        video = self.query('SELECT * FROM videos WHERE video_id=?', (offline.E,))[0]
        self.assertEqual(video['metadata_status'], 'ready')
        self.assertEqual(video['metadata_mode'], 'basic')
        self.assertEqual(video['resolution_status'], 'pending_vm')
        self.assertIsNone(video['error_code'])
        manifest = export_vmtask(self.root)
        tasks = [json.loads(line) for p in (self.root/'VMtask').glob('batch-*/tasks.jsonl') for line in p.read_text(encoding='utf-8').splitlines()]
        task = next(t for t in tasks if t['video_id']==offline.E)
        self.assertEqual(task['resolution_status'], 'pending_vm')
        metadata = json.loads((self.root/'VMtask'/f"batch-{task['assigned_part']:02d}"/task['metadata_path']).read_text(encoding='utf-8'))
        self.assertIsNone(metadata['resolution'])
        self.assertEqual(metadata['description'], 'Actual description')
        self.assertEqual(metadata['view_count'], 0)
        self.assertEqual(validate_vmtask(self.root/'VMtask')['metadata_count'], 3)

    def test_missing_like_is_still_held_and_never_filled_with_zero(self):
        self.save_basic(missing_like=True)
        self.assertEqual(self.query('SELECT metadata_status FROM videos WHERE video_id=?',(offline.E,))[0]['metadata_status'], 'partial')
        export_vmtask(self.root)
        held = [json.loads(line) for p in (self.root/'VMtask').glob('batch-*/held_tasks.jsonl') for line in p.read_text(encoding='utf-8').splitlines()]
        self.assertEqual(next(t for t in held if t['video_id']==offline.E)['missing_fields'], ['like_count'])

    def test_existing_estimate_and_registered_folder_are_preserved(self):
        import_existing(self.root)
        with closing(connect_database(self.data/'controller.sqlite3')) as db:
            with db:
                db.execute("UPDATE videos SET estimated_size_bytes=321,size_estimation_method='exact',folder_name='Frozen folder',folder_name_key='frozen folder' WHERE video_id=?",(offline.E,))
        self.save_basic()
        row = self.query('SELECT * FROM videos WHERE video_id=?',(offline.E,))[0]
        self.assertEqual(row['estimated_size_bytes'], 321)
        self.assertEqual(row['folder_name'], 'Frozen folder')

    def test_adapter_fallback_is_bounded_and_retains_basic_response(self):
        adapter = object.__new__(YouTubeAdapter)
        adapter.mode='basic';adapter.module=types.SimpleNamespace(canonical_url=lambda v:'https://www.youtube.com/watch?v='+v)
        basic=FakeAdapter().extract(offline.E);basic['raw']['like_count']=None
        full=FakeAdapter().extract(offline.E)
        with patch.object(adapter,'_extract_mode',side_effect=[basic,full]) as extract:
            result=adapter.extract(offline.E)
        self.assertEqual(extract.call_count,2)
        self.assertEqual(result['metadata_mode'],'full_fallback')
        self.assertIn('basic_raw',result)


if __name__=='__main__':unittest.main()
