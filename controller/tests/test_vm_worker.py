from contextlib import closing
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import unittest
from unittest.mock import patch

import test_offline_import as offline
from test_monitor_collection import FakeAdapter
from rmb_controller.database import connect_database
from rmb_controller.ingest import import_existing,utc_now
from rmb_controller.metadata import Collected,persist_collected
from rmb_controller.vmtask import export_vmtask

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from vm_worker.rmb_contract import load_batch
from vm_worker.rmb_uploader import TarStream,stream_upload
from vm_worker.worker import run_batch


class VMWorkerTests(unittest.TestCase):
    setUp=offline.OfflineImportTests.setUp

    def make_basic_batch(self,duration=1):
        import_existing(self.root)
        result=FakeAdapter().extract(offline.E);result.update(summary={},metadata_mode='basic')
        result['raw']['duration']=duration
        with closing(connect_database(self.data/'controller.sqlite3')) as db:
            stamp=utc_now()
            with db:
                db.execute("INSERT INTO collection_runs VALUES('basic',?,?,NULL,'running',1,0,'{}',NULL)",(stamp,stamp))
            persist_collected(db,self.data,'basic',Collected(offline.E,[{'at':stamp,'outcome':'success','code':None,'message':None,'retryable':False}],result))
            with db:db.execute("UPDATE collection_runs SET status='completed'")
        export_vmtask(self.root)
        for batch in (self.root/'VMtask').glob('batch-*'):
            _,manifest,tasks=load_batch(batch)
            if any(t['video_id']==offline.E for t in tasks):return batch
        self.fail('No basic task in export')

    def create_media(self):
        path=self.root/'generated-test.mkv'
        result=subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=128x72:r=10',
            '-f','lavfi','-i','sine=frequency=1000:sample_rate=44100','-t','1','-c:v','libx264','-c:a','aac','-shortest',str(path)],capture_output=True)
        self.assertEqual(result.returncode,0,result.stderr)
        return path

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),'ffmpeg required for real local QC test')
    def test_basic_metadata_flows_through_real_ffprobe_without_changing_source_or_folder(self):
        batch=self.make_basic_batch();source_bytes={p:p.read_bytes() for p in batch.glob('videos/*/metadata.json')}
        media=self.create_media();calls=[]
        def download(task,work,interval):
            calls.append(task['video_id']);shutil.copyfile(media,work/'001.mkv')
            return {'width':128,'height':72,'duration':1,'audio_expected':True,'format_id':'test'}
        result=run_batch(batch,self.root/'output',downloader=download)
        self.assertTrue(result['complete'])
        count=len(calls)
        repeated=run_batch(batch,self.root/'output',downloader=download)
        self.assertTrue(repeated['complete']);self.assertEqual(len(calls),count)
        for source,original in source_bytes.items():self.assertEqual(source.read_bytes(),original)
        for p in Path(result['output']).glob('videos/*/metadata.json'):
            final=json.loads(p.read_text(encoding='utf-8'));original=json.loads(source_bytes[batch/'videos'/p.parent.name/'metadata.json'])
            self.assertEqual(final['resolution'],'128x72')
            self.assertEqual({k:v for k,v in final.items() if k not in {'resolution','width','height'}},
                             {k:v for k,v in original.items() if k not in {'resolution','width','height'}})

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),'ffmpeg required')
    def test_wrong_resolution_never_publishes_delivery(self):
        batch=self.make_basic_batch();media=self.create_media()
        def download(task,work,interval):
            shutil.copyfile(media,work/'001.mkv');return {'width':1920,'height':1080,'duration':1}
        result=run_batch(batch,self.root/'output',downloader=download)
        self.assertFalse(result['complete'])
        self.assertEqual(list(Path(result['output']).glob('videos/*/001.mkv')),[])

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),'ffmpeg required')
    def test_changed_source_duration_never_publishes_stale_client_metadata(self):
        batch=self.make_basic_batch(duration=30);media=self.create_media()
        def download(task,work,interval):
            shutil.copyfile(media,work/'001.mkv');return {'width':128,'height':72,'duration':1}
        result=run_batch(batch,self.root/'output',downloader=download)
        self.assertFalse(result['complete'])
        self.assertEqual(list(Path(result['output']).glob('videos/*/metadata.json')),[])

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),'ffmpeg required')
    def test_verified_upload_cleanup_and_resume_never_redownload_or_change_source(self):
        batch=self.make_basic_batch();media=self.create_media();downloads=[];uploads=[]
        def download(task,work,interval):
            downloads.append(task['video_id']);shutil.copyfile(media,work/'001.mkv')
            return {'width':128,'height':72,'duration':1,'audio_expected':True}
        def upload(files,delivery,key,**kwargs):
            uploads.append((files,delivery,key));self.assertTrue(all(p.is_file() for p,relative in files))
            return {'key':key,'bytes':123,'bucket':'fake'}
        config={'prefix':'test','bucket':'fake'}
        with patch('vm_worker.worker.upload_config',return_value=config):
            result=run_batch(batch,self.root/'output',downloader=download,upload=True,
                             delete_after_upload=True,chunk_bytes=1,uploader=upload)
            repeated=run_batch(batch,self.root/'output',downloader=download,upload=True,
                               delete_after_upload=True,chunk_bytes=1,uploader=upload)
        self.assertTrue(result['complete']);self.assertTrue(repeated['complete'])
        self.assertEqual(len(downloads),1);self.assertEqual(len(uploads),1)
        self.assertEqual(list(Path(result['output']).glob('videos/*/001.mkv')),[])
        self.assertTrue(any(batch.glob('videos/*/metadata.json')))

    def test_tar_stream_preserves_unicode_names_and_only_given_files(self):
        path=self.root/'metadata-test.json';path.write_text('{"ok":true}',encoding='utf-8')
        stream=TarStream([(path,'測試 title/metadata.json')],'SUPPLIER-v1')
        try:
            blocks=[]
            while block:=stream.read(4096):blocks.append(block)
        finally:stream.close()
        with tarfile.open(fileobj=io.BytesIO(b''.join(blocks))) as archive:
            self.assertEqual(archive.getnames(),['SUPPLIER-v1/測試 title/metadata.json'])
            self.assertEqual(archive.extractfile(archive.getmembers()[0]).read(),path.read_bytes())

    def test_mock_upload_checks_size_and_reuses_matching_object(self):
        path=self.root/'metadata-test.json';path.write_text('{}',encoding='utf-8')
        class Missing(Exception):response={'Error':{'Code':'404'}}
        class Client:
            body=None;metadata={};uploads=0
            def head_object(self,**kwargs):
                if self.body is None:raise Missing()
                return {'ContentLength':len(self.body),'Metadata':self.metadata,'ETag':'test'}
            def upload_fileobj(self,stream,bucket,key,ExtraArgs,Config):
                self.uploads+=1;parts=[]
                while block:=stream.read(4096):parts.append(block)
                self.body=b''.join(parts);self.metadata=ExtraArgs['Metadata']
        client=Client();config={'bucket':'test','endpoint':'https://unused.invalid','region':'test','prefix':'test'}
        args=([(path,'title/metadata.json')],'delivery','test.tar')
        first=stream_upload(*args,config=config,client=client,fingerprint='same',transfer_config=object())
        second=stream_upload(*args,config=config,client=client,fingerprint='same',transfer_config=object())
        self.assertEqual(first['bytes'],len(client.body));self.assertTrue(second['reused_verified_object']);self.assertEqual(client.uploads,1)

    def test_upload_package_contains_worker_and_exact_task_metadata(self):
        batch=self.make_basic_batch()
        package=self.root/'VMpackages/packages'/(batch.name+'.tar.gz')
        with tarfile.open(package,'r:gz') as archive:
            names=archive.getnames()
            self.assertIn('worker/worker.py',names)
            self.assertIn('worker/Dockerfile',names)
            self.assertIn(batch.name+'/tasks.jsonl',names)
            self.assertFalse(any('/.controller.json' in n or n.endswith('/oss.env') for n in names))
            for source in batch.glob('videos/*/metadata.json'):
                member=batch.name+'/'+source.relative_to(batch).as_posix()
                self.assertEqual(archive.extractfile(member).read(),source.read_bytes())

    def test_untrusted_export_id_cannot_escape_output_root(self):
        batch=self.make_basic_batch();path=batch/'manifest.json'
        manifest=json.loads(path.read_text(encoding='utf-8'));manifest['export_id']='../../outside'
        path.write_text(json.dumps(manifest),encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'Unsafe export'):
            load_batch(batch)


if __name__=='__main__':unittest.main()
