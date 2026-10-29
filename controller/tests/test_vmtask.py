from contextlib import closing
import json
import shutil
import sys
import unittest
import zipfile
from unittest.mock import patch

import test_offline_import as offline
from rmb_controller.artifacts import sha256
from rmb_controller.database import connect_database
from rmb_controller.ingest import SAMPLE_ROOT, import_existing
from rmb_controller.vmtask import append_live_failure, export_vmtask, remove_legacy_samples, validate_vmtask
from rmb_controller.metadata import collect_metadata
from test_monitor_collection import FakeAdapter


def lock_read_only(path):
    """Open `path` with read-only sharing, so other processes cannot write or replace it."""
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                                   wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.CreateFileW(str(path), 0x80000000, 1, None, 3, 0x80, None)  # GENERIC_READ, SHARE_READ, OPEN_EXISTING
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    return lambda: kernel.CloseHandle(handle)


class VMTaskTests(unittest.TestCase):
    setUp = offline.OfflineImportTests.setUp
    query = offline.OfflineImportTests.query

    def test_all_twenty_batches_preserve_json_and_separate_missing_fields(self):
        import_existing(self.root)
        originals = {p.parent.name: p.read_bytes() for p in (self.root / SAMPLE_ROOT).glob('*/metadata.json')}
        result = export_vmtask(self.root)
        root = self.root / 'VMtask'
        self.assertEqual((result['metadata_count'], result['ready_count'], result['held_count']), (2, 1, 1))
        self.assertEqual(result['pending_metadata_count'], 3)
        self.assertEqual(result['bad_file_count'], 1)
        self.assertEqual(len(list(root.glob('batch-*'))), 20)
        self.assertEqual(validate_vmtask(root)['metadata_count'], 2)
        for path in root.glob('batch-*/videos/*/metadata.json'):
            self.assertEqual(path.read_bytes(), originals[path.parent.name])
            self.assertEqual([p.name for p in path.parent.iterdir()], ['metadata.json'])
        held = [json.loads(line) for p in root.glob('batch-*/held_tasks.jsonl') for line in p.read_text(encoding='utf-8').splitlines()]
        self.assertEqual(held[0]['missing_fields'], ['like_count'])
        bad = [json.loads(line) for line in (root / 'bad_files_current.jsonl').read_text(encoding='utf-8').splitlines()]
        self.assertEqual([r['video_id'] for r in bad], [offline.A])
        self.assertFalse(bad[0]['permanent'])
        self.assertEqual(len(self.query('SELECT video_id FROM videos WHERE assigned_part IS NOT NULL')), 5)

    def test_metadata_or_folder_tampering_is_detected(self):
        import_existing(self.root)
        export_vmtask(self.root)
        root = self.root / 'VMtask'
        path = next(root.glob('batch-*/videos/*/metadata.json'))
        path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            validate_vmtask(root)

    def test_customer_sample_json_migrates_before_exact_media_deletion(self):
        import_existing(self.root)
        export_vmtask(self.root)
        result = remove_legacy_samples(self.root)
        self.assertEqual(result['sample_folders_removed'], 2)
        self.assertFalse((self.root / SAMPLE_ROOT).exists())
        self.assertEqual(validate_vmtask(self.root / 'VMtask')['metadata_count'], 2)
        self.assertTrue(all(r['download_status'] == 'not_started' for r in self.query('SELECT download_status FROM videos')))
        log = self.root / 'VMtask/legacy_sample_cleanup.jsonl'
        self.assertEqual(len(log.read_text(encoding='utf-8').splitlines()), 2)

    def test_unexpected_sample_file_prevents_all_deletion(self):
        import_existing(self.root)
        export_vmtask(self.root)
        (self.root / SAMPLE_ROOT / 'Complete/notes.txt').write_text('keep', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'Unexpected contents'):
            remove_legacy_samples(self.root)
        self.assertEqual(len(list((self.root / SAMPLE_ROOT).glob('*/001.mkv'))), 2)

    def test_refresh_preserves_previous_export_and_fixed_parts(self):
        import_existing(self.root)
        first = export_vmtask(self.root)
        before = self.query('SELECT video_id,assigned_part FROM videos')
        second = export_vmtask(self.root)
        archive = self.root / self.query('SELECT relative_path FROM exports WHERE export_id=?',(first['export_id'],))[0]['relative_path']
        self.assertNotEqual(first['export_id'], second['export_id'])
        self.assertEqual(archive.suffix,'.zip')
        with zipfile.ZipFile(archive) as saved:
            self.assertEqual(json.loads(saved.read('manifest.json'))['export_id'],first['export_id'])
        self.assertEqual({p.name for p in (self.root/'VMtask').iterdir() if p.is_dir()},
                         {f'batch-{n:02d}' for n in range(1,21)})
        self.assertEqual(validate_vmtask(self.root/'VMtask')['metadata_count'],2)
        self.assertEqual(self.query('SELECT video_id,assigned_part FROM videos'), before)
        self.assertEqual(len(self.query('SELECT * FROM exports')), 2)

    def test_preexisting_unmanaged_files_are_never_overwritten(self):
        import_existing(self.root)
        root = self.root / 'VMtask'
        root.mkdir()
        (root / 'keep.txt').write_text('keep', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'unmanaged'):
            export_vmtask(self.root)
        self.assertEqual((root / 'keep.txt').read_text(encoding='utf-8'), 'keep')
        self.assertEqual(self.query('SELECT COUNT(*) AS n FROM videos WHERE assigned_part IS NOT NULL')[0]['n'], 0)

    def test_new_metadata_merges_without_moving_batches_or_rewriting_unchanged_files(self):
        import_existing(self.root)
        export_vmtask(self.root)
        root=self.root/'VMtask'
        directories={p.name:p.stat().st_ino for p in root.glob('batch-*')}
        originals={p:(p.read_bytes(),p.stat().st_mtime_ns) for p in root.glob('batch-*/videos/*/metadata.json')}
        bindings=self.query('SELECT video_id,folder_name,assigned_part FROM videos WHERE folder_name IS NOT NULL')
        collect_metadata(self.root,self.data,limit=1,adapter=FakeAdapter(),progress=lambda *a,**kw:None)
        result=export_vmtask(self.root)
        self.assertEqual(result['metadata_count'],3)
        self.assertEqual({p.name:p.stat().st_ino for p in root.glob('batch-*')},directories)
        for p,(payload,mtime) in originals.items():
            self.assertEqual(p.read_bytes(),payload)
            self.assertEqual(p.stat().st_mtime_ns,mtime)
        after={r['video_id']:r for r in self.query('SELECT video_id,folder_name,assigned_part FROM videos')}
        for before in bindings:self.assertEqual(after[before['video_id']],before)
        self.assertEqual(validate_vmtask(root)['metadata_count'],3)

    def test_partially_moved_legacy_history_is_archived_and_consolidated(self):
        import_existing(self.root)
        first=export_vmtask(self.root)
        export_vmtask(self.root)
        with closing(connect_database(self.data/'controller.sqlite3')) as db, db:
            db.execute('UPDATE exports SET relative_path=? WHERE export_id=?',
                       ('VMtask/_history/'+first['export_id'],first['export_id']))
        root=self.root/'VMtask'
        history=root/'_history'/first['export_id'];history.mkdir(parents=True)
        for part in range(1,17):(root/f'batch-{part:02d}').rename(history/f'batch-{part:02d}')
        pending=root/'.b-interrupted';pending.mkdir()
        (pending/'notes.json').write_text('{"saved":true}',encoding='utf-8')
        result=export_vmtask(self.root,consolidate=True)
        self.assertEqual(result['metadata_count'],2)
        self.assertEqual(len([p for p in root.iterdir() if p.is_dir()]),20)
        self.assertFalse((root/'_history').exists())
        backup=next((self.data/'vmtask_archive').glob('*-legacy-tree.zip'))
        with zipfile.ZipFile(backup) as archive:
            self.assertEqual(archive.read('.b-interrupted/notes.json'),b'{"saved":true}')
            self.assertTrue(any(n.startswith('_history/') for n in archive.namelist()))
        self.assertEqual(validate_vmtask(root)['metadata_count'],2)
        for old in self.query("SELECT relative_path FROM exports WHERE relative_path!='VMtask'"):
            self.assertTrue((self.root/old['relative_path']).is_file())

    def test_interrupted_in_place_update_can_be_retried(self):
        import_existing(self.root)
        export_vmtask(self.root)
        with patch('rmb_controller.publication.atomic_file',side_effect=OSError('simulated write failure')):
            with self.assertRaisesRegex(OSError,'simulated write failure'):export_vmtask(self.root)
        self.assertFalse((self.root/'VMtask/READY.json').exists())
        self.assertTrue((self.data/'vmtask_archive/pending_publication.json').exists())
        export_vmtask(self.root)
        self.assertEqual(validate_vmtask(self.root/'VMtask')['metadata_count'],2)
        self.assertFalse((self.data/'vmtask_archive/pending_publication.json').exists())

    def stage_dirs(self):
        return list(self.root.glob('.vm-stage-*')) + list(self.data.glob('vmtask_stage*'))

    def test_export_leaves_no_staging_folder_behind(self):
        import_existing(self.root)
        export_vmtask(self.root)
        export_vmtask(self.root)
        self.assertEqual(self.stage_dirs(), [])

    def test_failed_publications_do_not_accumulate_staging_folders(self):
        import_existing(self.root)
        export_vmtask(self.root)
        for _ in range(3):
            with patch('rmb_controller.publication.atomic_file', side_effect=OSError('simulated write failure')):
                with self.assertRaises(OSError):export_vmtask(self.root)
            self.assertEqual(self.stage_dirs(), [])
        export_vmtask(self.root)
        self.assertEqual(validate_vmtask(self.root / 'VMtask')['metadata_count'], 2)

    def test_stale_staging_folders_from_older_versions_are_removed(self):
        import_existing(self.root)
        stale = self.root / '.vm-stage-deadbeef'
        (stale / 'VMtask').mkdir(parents=True)
        (stale / 'VMtask' / 'x.json').write_text('{}', encoding='utf-8')
        unrelated = self.root / '.vm-stage-keepme'  # not our naming; must never be touched
        unrelated.mkdir()
        export_vmtask(self.root)
        self.assertFalse(stale.exists())
        self.assertTrue(unrelated.exists())

    @unittest.skipUnless(sys.platform == 'win32', 'file sharing locks are a Windows behaviour')
    def test_file_open_in_another_program_is_reported_before_anything_changes(self):
        import_existing(self.root)
        export_vmtask(self.root)
        root = self.root / 'VMtask'
        collect_metadata(self.root, self.data, limit=1, adapter=FakeAdapter(), progress=lambda *a, **kw: None)
        before = {p.name: p.read_bytes() for p in root.glob('*.csv')}
        release = lock_read_only(root / 'inventory.csv')  # what Excel/WPS do while a CSV is open
        try:
            with self.assertRaisesRegex(ValueError, 'inventory.csv'):export_vmtask(self.root)
        finally:
            release()
        self.assertTrue((root / 'READY.json').is_file())  # previous snapshot still dispatchable
        self.assertFalse((self.data / 'vmtask_archive/pending_publication.json').exists())
        self.assertEqual({p.name: p.read_bytes() for p in root.glob('*.csv')}, before)
        self.assertEqual(self.stage_dirs(), [])
        self.assertEqual(export_vmtask(self.root)['metadata_count'], 3)

    def test_live_temporary_errors_are_logged_separately_without_invalidating_export(self):
        import_existing(self.root)
        export_vmtask(self.root)
        with closing(connect_database(self.data / 'controller.sqlite3')) as db:
            with db:
                db.execute("UPDATE videos SET metadata_status='retry_wait',error_code='NETWORK_TRANSIENT' WHERE video_id=?", (offline.E,))
            append_live_failure(db, self.root, offline.E, 'test-run')
        event = json.loads((self.root / 'VMtask/bad_files_live_failures.jsonl').read_text(encoding='utf-8'))
        self.assertTrue(event['retryable'])
        self.assertFalse(event['permanent'])
        self.assertEqual(validate_vmtask(self.root / 'VMtask')['metadata_count'], 2)

    def test_recent_access_failure_holds_a_previously_complete_metadata_record(self):
        import_existing(self.root)
        with closing(connect_database(self.data / 'controller.sqlite3')) as db:
            with db:
                db.execute("UPDATE videos SET metadata_status='restricted',error_code='PRIVATE_VIDEO' WHERE video_id=?", (offline.D,))
        result = export_vmtask(self.root)
        self.assertEqual(result['ready_count'], 0)
        self.assertEqual(result['held_count'], 2)
        self.assertEqual(validate_vmtask(self.root / 'VMtask')['held_count'], 2)


if __name__ == '__main__':
    unittest.main()
