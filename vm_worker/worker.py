#!/usr/bin/env python3
"""Download each task once, verify with ffprobe, publish immutable-name output, optionally upload TAR chunks."""
import argparse
from contextlib import contextmanager
import csv
from fractions import Fraction
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import time

try:
    from .rmb_contract import FORMAT, load_batch, required_missing, safe_name, sha256_file
    from .rmb_uploader import stream_upload, upload_config
except ImportError:
    from rmb_contract import FORMAT, load_batch, required_missing, safe_name, sha256_file
    from rmb_uploader import stream_upload, upload_config


def now():return datetime.now(timezone.utc).isoformat(timespec='seconds')


def encode(value):return (json.dumps(value,ensure_ascii=False,allow_nan=False,indent=2)+'\n').encode('utf-8')


def write_new(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())


def append(path,value):
    with Path(path).open('a',encoding='utf-8') as f:
        f.write(json.dumps(value,ensure_ascii=False)+'\n');f.flush();os.fsync(f.fileno())


@contextmanager
def exclusive(path):
    with Path(path).open('a+b') as handle:
        if handle.tell()==0:handle.write(b'0');handle.flush()
        handle.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError as exc:raise ValueError('Another worker is using this output batch') from exc
        try:yield
        finally:
            handle.seek(0)
            if os.name=='nt':msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)
            else:fcntl.flock(handle.fileno(),fcntl.LOCK_UN)


def error_category(message):
    text=str(message).lower()
    if re.search(r'\b429\b|too many requests|rate.limit',text):return 'HTTP_429',True
    if 'private video' in text:return 'PRIVATE_VIDEO',False
    if 'removed by the uploader' in text or 'has been removed' in text:return 'VIDEO_REMOVED',False
    if 'sign in' in text or 'not available in your country' in text:return 'ACCESS_RESTRICTED',False
    if re.search(r'timeout|timed out|connection|dns|\b(?:408|500|502|503|504)\b',text):return 'NETWORK_TRANSIENT',True
    if 'qc:' in text:return 'QC_FAILED',False
    if isinstance(message,OSError) or 'no space left' in text or 'disk quota' in text:return 'LOCAL_IO_ERROR',False
    if 'unavailable' in text or re.search(r'\b(?:401|403|404|410)\b',text):return 'AVAILABILITY_UNKNOWN',False
    return 'DOWNLOAD_ERROR',False


def ffprobe(path):
    command=['ffprobe','-v','error','-show_entries','stream=codec_type,codec_name,width,height,avg_frame_rate:format=duration,size','-of','json',str(path)]
    result=subprocess.run(command,capture_output=True,text=True,encoding='utf-8',timeout=120)
    if result.returncode:raise ValueError('QC: ffprobe failed: '+result.stderr[-500:])
    payload=json.loads(result.stdout)
    video=next((s for s in payload.get('streams',[]) if s.get('codec_type')=='video'),None)
    if not video or video.get('width',0)<=0 or video.get('height',0)<=0:
        raise ValueError('QC: no valid video stream')
    duration=float(payload.get('format',{}).get('duration') or 0)
    if duration<=0:raise ValueError('QC: no positive duration')
    try:fps=float(Fraction(video.get('avg_frame_rate') or '0'))
    except (ValueError,ZeroDivisionError):fps=0
    return {'width':video['width'],'height':video['height'],'duration':duration,'fps':fps,
            'codec':video.get('codec_name'),'audio_present':any(s.get('codec_type')=='audio' for s in payload['streams'])}


def yt_download(task,work,request_interval=1):
    import yt_dlp
    runtimes={name:{'path':path} for name in ('deno','node','qjs') if (path:=shutil.which(name))}
    if 'qjs' in runtimes:runtimes['quickjs']=runtimes.pop('qjs')
    if not runtimes:raise ValueError('Install Node >=22 or Deno >=2.3 before downloading')
    last=[0.0]
    def progress(update):
        stamp=time.monotonic()
        if update['status']=='downloading' and stamp-last[0]>2:
            last[0]=stamp
            total=update.get('total_bytes') or update.get('total_bytes_estimate')
            percentage=100*update.get('downloaded_bytes',0)/total if total else 0
            print(f"DOWNLOAD {task['video_id']} {percentage:.1f}%",flush=True)
    options={'format':FORMAT,'format_sort':['res','fps'],'format_sort_force':True,
        'outtmpl':str(work/'001.%(ext)s'),'merge_output_format':'mkv',
        'postprocessors':[{'key':'FFmpegVideoRemuxer','preferedformat':'mkv'}],
        'noplaylist':True,'quiet':True,'noprogress':True,'progress_hooks':[progress],
        'writethumbnail':False,'writesubtitles':False,'writeautomaticsub':False,'writeinfojson':False,
        'getcomments':False,'cachedir':False,'socket_timeout':30,'retries':0,'extractor_retries':0,
        'fragment_retries':0,'continuedl':True,'overwrites':False,'sleep_interval_requests':request_interval,
        'js_runtimes':runtimes,'remote_components':[],'cookiefile':None,'cookiesfrombrowser':None,
        'usenetrc':False,'proxy':'','geo_bypass':False}
    # One call selects the highest available streams AND performs the download.
    with yt_dlp.YoutubeDL(options) as ydl:
        info=ydl.extract_info(task['url'],download=True)
    if not isinstance(info,dict) or info.get('id')!=task['video_id']:
        raise ValueError('Downloaded video identity differs from task')
    selected=next((f for f in info.get('requested_formats',[]) if f.get('vcodec') not in (None,'none')),info)
    return {'width':selected.get('width'),'height':selected.get('height'),
            'fps':selected.get('fps'),'duration':info.get('duration'),
            'audio_expected':any(f.get('acodec') not in (None,'none') for f in info.get('requested_formats',[])) or info.get('acodec') not in (None,'none'),
            'format_id':info.get('format_id')}


def open_state(path,manifest,tasks,batch):
    db=sqlite3.connect(path);db.row_factory=sqlite3.Row
    db.executescript('''CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS tasks(video_id TEXT PRIMARY KEY,status TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
        quality_json TEXT,error_code TEXT,error_message TEXT,uploaded_key TEXT,updated_at_utc TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS uploads(fingerprint TEXT PRIMARY KEY,result_json TEXT NOT NULL);''')
    identity=json.dumps({'export_id':manifest['export_id'],'part':manifest['assigned_part'],
                         'tasks_sha256':sha256_file(batch/'tasks.jsonl')},sort_keys=True)
    previous=db.execute("SELECT value FROM settings WHERE key='batch_identity'").fetchone()
    if previous and previous[0]!=identity:db.close();raise ValueError('Output state belongs to a different task snapshot')
    with db:
        db.execute("INSERT OR IGNORE INTO settings VALUES('batch_identity',?)",(identity,))
        db.executemany("INSERT OR IGNORE INTO tasks(video_id,status,updated_at_utc) VALUES(?,'pending',?)",((t['video_id'],now()) for t in tasks))
    return db


def finish_publish(output,state_dir,task,quality):
    target=output/'videos'/task['folder_name']
    if not target.resolve().is_relative_to(output.resolve()) or target.is_symlink():raise ValueError('Output folder escapes batch')
    publish=state_dir/'publish'/task['video_id']
    temp=state_dir/'downloads'/task['video_id']/'001.mkv'
    def verify(folder):
        return (folder/'001.mkv').is_file() and (folder/'metadata.json').is_file() and \
            sha256_file(folder/'001.mkv')==quality['media_sha256'] and sha256_file(folder/'metadata.json')==quality['metadata_sha256']
    if target.exists():
        if target.is_symlink() or not verify(target):raise ValueError('QC: existing output differs; refusing overwrite')
        return target
    if not target.resolve().is_relative_to(output.resolve()):raise ValueError('Output folder escapes batch')
    publish.mkdir(parents=True,exist_ok=True)
    media=publish/'001.mkv'
    if not media.exists():
        if not temp.is_file() or sha256_file(temp)!=quality['media_sha256']:raise ValueError('QC: missing recoverable media')
        temp.rename(media)
    metadata=publish/'metadata.json'
    if not metadata.exists():write_new(metadata,encode(quality['customer_metadata']))
    if not verify(publish):raise ValueError('QC: publication hashes differ')
    target.parent.mkdir(parents=True,exist_ok=True)
    publish.rename(target)
    return target


def process_task(db,batch,output,state_dir,task,downloader,verify_hashes=False,interval=1):
    row=db.execute('SELECT * FROM tasks WHERE video_id=?',(task['video_id'],)).fetchone()
    if row['status']=='uploaded':return 'uploaded'
    if row['quality_json']:
        quality=json.loads(row['quality_json'])
        target=output/'videos'/task['folder_name']
        if row['status']=='completed' and target.is_dir():
            media=target/'001.mkv';stat=media.stat()
            if stat.st_size==quality['media_bytes'] and stat.st_mtime_ns==quality.get('media_mtime_ns') and sha256_file(target/'metadata.json')==quality['metadata_sha256']:
                if not verify_hashes or sha256_file(media)==quality['media_sha256']:return 'completed'
        if row['status']=='qc_ready':
            target=finish_publish(output,state_dir,task,quality)
        else:raise ValueError('QC: previously completed output changed; refusing overwrite')
    else:
        with db:db.execute("UPDATE tasks SET status='downloading',attempts=attempts+1,updated_at_utc=? WHERE video_id=?",(now(),task['video_id']))
        work=state_dir/'downloads'/task['video_id'];work.mkdir(parents=True,exist_ok=True)
        selected=downloader(task,work,interval)
        media=work/'001.mkv'
        if not media.is_file() or media.is_symlink():raise ValueError('QC: downloader did not produce 001.mkv')
        measured=ffprobe(media)
        expected=(selected.get('width'),selected.get('height'))
        actual=(measured['width'],measured['height'])
        if not all(expected) or expected not in (actual,actual[::-1]):raise ValueError(f'QC: selected resolution {expected} differs from ffprobe {actual}')
        if selected.get('fps') and measured['fps'] and abs(selected['fps']-measured['fps'])>max(.2,selected['fps']*.01):raise ValueError('QC: selected fps differs from ffprobe')
        if selected.get('audio_expected') and not measured['audio_present']:raise ValueError('QC: selected audio is missing')
        duration=selected.get('duration')
        if duration and abs(measured['duration']-duration)>max(3,duration*.02):raise ValueError('QC: downloaded duration differs from selected source')
        metadata=json.loads((batch/task['metadata_path']).read_text(encoding='utf-8-sig'))
        if abs(measured['duration']-metadata['duration'])>max(3,metadata['duration']*.02):
            raise ValueError('QC: source duration changed since local metadata; refresh is required')
        metadata.update(resolution=f"{actual[0]}x{actual[1]}",width=actual[0],height=actual[1])
        if required_missing(metadata):raise ValueError('QC: final customer metadata incomplete')
        quality={'verified_at_utc':now(),'resolution_status':'verified_vm','selected':selected,'ffprobe':measured,
                 'customer_metadata':metadata,'media_bytes':media.stat().st_size,'media_sha256':sha256_file(media),
                 'metadata_sha256':hashlib.sha256(encode(metadata)).hexdigest()}
        with db:db.execute("UPDATE tasks SET status='qc_ready',quality_json=?,error_code=NULL,error_message=NULL WHERE video_id=?",(json.dumps(quality,ensure_ascii=False),task['video_id']))
        target=finish_publish(output,state_dir,task,quality)
    quality['media_mtime_ns']=(target/'001.mkv').stat().st_mtime_ns
    with db:db.execute("UPDATE tasks SET status='completed',quality_json=?,error_code=NULL,error_message=NULL,updated_at_utc=? WHERE video_id=?",(json.dumps(quality,ensure_ascii=False),now(),task['video_id']))
    append(state_dir/'events.jsonl',{'at':now(),'video_id':task['video_id'],'event':'qc_passed','resolution':quality['customer_metadata']['resolution']})
    return 'completed'


def upload_chunk(db,output,state_dir,tasks,manifest,delivery_name,config,delete_after=False,uploader=stream_upload):
    files=[];entries=[]
    for task in tasks:
        row=db.execute('SELECT * FROM tasks WHERE video_id=?',(task['video_id'],)).fetchone()
        quality=json.loads(row['quality_json']);folder=output/'videos'/task['folder_name']
        if folder.is_symlink() or not folder.resolve().is_relative_to(output.resolve()):raise ValueError('Upload path escapes batch')
        for name,expected in [('001.mkv',quality['media_sha256']),('metadata.json',quality['metadata_sha256'])]:
            if sha256_file(folder/name)!=expected:raise ValueError('QC: file changed before upload')
            files.append((folder/name,task['folder_name']+'/'+name))
        entries.append({'video_id':task['video_id'],'folder_name':task['folder_name'],
                        'media_sha256':quality['media_sha256'],'metadata_sha256':quality['metadata_sha256']})
    fingerprint=hashlib.sha256(encode(entries)).hexdigest()
    key='/'.join(x for x in [config['prefix'],manifest['export_id'],f"batch-{manifest['assigned_part']:02d}",fingerprint+'.tar'] if x)
    result=uploader(files,delivery_name,key,config=config,fingerprint=fingerprint)
    with db:
        db.execute('INSERT OR REPLACE INTO uploads VALUES(?,?)',(fingerprint,json.dumps(result)))
        db.executemany("UPDATE tasks SET status='uploaded',uploaded_key=?,updated_at_utc=? WHERE video_id=?",((key,now(),t['video_id']) for t in tasks))
    append(state_dir/'uploads.jsonl',{'at':now(),'fingerprint':fingerprint,'entries':entries,**result})
    if delete_after:
        for task in tasks:
            folder=output/'videos'/task['folder_name']
            if folder.is_symlink() or folder.resolve().parent!=(output/'videos').resolve() or set(p.name for p in folder.iterdir())!={'metadata.json','001.mkv'}:
                raise ValueError('Refusing cleanup outside exact uploaded delivery folders')
        for task in tasks:
            folder=output/'videos'/task['folder_name'];(folder/'001.mkv').unlink();(folder/'metadata.json').unlink();folder.rmdir()
    return result


def run_batch(batch_path,output_root,*,limit=None,upload=False,delete_after_upload=False,chunk_bytes=20*1024**3,
              interval=1,retry_failed=False,verify_hashes=False,delivery_name='SUPPLIER-v1-1028-youtube_camera_control',
              downloader=None,uploader=stream_upload):
    batch,manifest,tasks=load_batch(batch_path)
    if not safe_name(delivery_name):raise ValueError('Unsafe delivery name')
    output_base=Path(output_root).resolve()
    output=output_base/manifest['export_id']/f"batch-{manifest['assigned_part']:02d}"
    if output.is_symlink() or not output.resolve().is_relative_to(output_base):raise ValueError('Output escapes output-root')
    if output.is_relative_to(batch) or batch.is_relative_to(output):raise ValueError('Output must be separate from immutable batch input')
    if chunk_bytes<=0 or interval<1 or limit is not None and limit<1:raise ValueError('Invalid run options')
    if delete_after_upload and not upload:raise ValueError('--delete-after-upload requires --upload')
    config=upload_config() if upload else None
    if downloader is None:
        if not shutil.which('ffmpeg') or not shutil.which('ffprobe'):raise ValueError('Install ffmpeg and ffprobe')
        downloader=yt_download
    state_dir=output/'.worker'
    if state_dir.is_symlink() or (output/'videos').is_symlink():raise ValueError('Symlink state/output directory is not allowed')
    state_dir.mkdir(parents=True,exist_ok=True)
    failures=0;handled=0;chunk=[];chunk_size=0
    with exclusive(state_dir/'run.lock'):
        db=open_state(state_dir/'state.sqlite3',manifest,tasks,batch)
        try:
            for position,task in enumerate(tasks,1):
                row=db.execute('SELECT * FROM tasks WHERE video_id=?',(task['video_id'],)).fetchone()
                if row['status']=='uploaded':continue
                if row['status']=='failed' and not retry_failed:failures+=1;continue
                cached=row['status']=='completed'
                if not cached and limit is not None and handled>=limit:break
                if not cached:handled+=1
                print(f"TASK {position}/{len(tasks)} {task['video_id']} -> {task['folder_name']}",flush=True)
                try:
                    status=process_task(db,batch,output,state_dir,task,downloader,verify_hashes,interval)
                    if status=='completed' and upload:
                        chunk.append(task);quality=json.loads(db.execute('SELECT quality_json FROM tasks WHERE video_id=?',(task['video_id'],)).fetchone()[0]);chunk_size+=quality['media_bytes']
                except KeyboardInterrupt:raise
                except Exception as exc:
                    code,retryable=error_category(exc);failures+=1
                    with db:db.execute("UPDATE tasks SET status='failed',error_code=?,error_message=?,updated_at_utc=? WHERE video_id=?",(code,str(exc),now(),task['video_id']))
                    append(state_dir/'failures.jsonl',{'at':now(),'video_id':task['video_id'],'url':task['url'],'folder_name':task['folder_name'],'error_code':code,'retryable':retryable,'error':str(exc)})
                    print(f"FAILED {task['video_id']} {code}: {exc}",flush=True)
                    if code in {'HTTP_429','LOCAL_IO_ERROR'}:break
                if upload and chunk_size>=chunk_bytes:
                    # An OSS error must leave QC-complete tasks intact for upload-only resume.
                    upload_chunk(db,output,state_dir,chunk,manifest,delivery_name,config,delete_after_upload,uploader)
                    chunk=[];chunk_size=0
            if upload and chunk:
                upload_chunk(db,output,state_dir,chunk,manifest,delivery_name,config,delete_after_upload,uploader)
            counts=dict(db.execute('SELECT status,COUNT(*) FROM tasks GROUP BY status'))
            complete=sum(counts.get(k,0) for k in ('completed','uploaded'))==len(tasks)
            report={'export_id':manifest['export_id'],'batch':manifest['assigned_part'],'output':str(output),
                    'tasks':len(tasks),'held_tasks_not_processed':manifest['held_count'],'counts':counts,'complete':complete,'updated_at_utc':now()}
            (state_dir/'summary.json').write_bytes(encode(report))
            with (state_dir/'results.csv').open('w',encoding='utf-8-sig',newline='') as f:
                writer=csv.writer(f);writer.writerow(['video_id','status','attempts','error_code','error_message','uploaded_key'])
                writer.writerows(tuple(r[k] for k in ('video_id','status','attempts','error_code','error_message','uploaded_key')) for r in db.execute('SELECT * FROM tasks ORDER BY video_id'))
            return report
        finally:db.close()


def main():
    parser=argparse.ArgumentParser(description='RMB VM Worker: download -> merge -> ffprobe -> update metadata -> optional TAR stream upload')
    parser.add_argument('--batch',required=True,type=Path)
    parser.add_argument('--output-root',type=Path,default=Path('./output'))
    parser.add_argument('--validate-only',action='store_true');parser.add_argument('--limit',type=int)
    parser.add_argument('--upload',action='store_true');parser.add_argument('--delete-after-upload',action='store_true')
    parser.add_argument('--chunk-gib',type=float,default=20);parser.add_argument('--request-interval',type=float,default=1)
    parser.add_argument('--retry-failed',action='store_true');parser.add_argument('--verify-hashes',action='store_true')
    parser.add_argument('--delivery-name',default='SUPPLIER-v1-1028-youtube_camera_control')
    args=parser.parse_args()
    try:
        if args.validate_only:
            batch,manifest,tasks=load_batch(args.batch);print(json.dumps({'valid':True,'tasks':len(tasks),'held':manifest['held_count']},ensure_ascii=False));return 0
        result=run_batch(args.batch,args.output_root,limit=args.limit,upload=args.upload,delete_after_upload=args.delete_after_upload,
            chunk_bytes=int(args.chunk_gib*1024**3),interval=args.request_interval,retry_failed=args.retry_failed,
            verify_hashes=args.verify_hashes,delivery_name=args.delivery_name)
        print(json.dumps(result,ensure_ascii=False,indent=2));return 0 if result['complete'] else 2
    except KeyboardInterrupt:print('Stopped; partial downloads and state retained. Resume with the same command.',file=sys.stderr);return 130
    except Exception as exc:print('ERROR: '+str(exc),file=sys.stderr);return 1


if __name__=='__main__':raise SystemExit(main())
