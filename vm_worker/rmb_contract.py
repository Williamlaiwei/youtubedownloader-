"""Task contract shared with Controller exports; no Controller or model dependency."""
import hashlib
import json
from pathlib import Path
import re
import unicodedata

FORMAT = 'bestvideo*+bestaudio/best'


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def safe_name(value):
    return (isinstance(value,str) and value not in {'','.','..'} and
            not any(c in value for c in '\\/:<>"|?*') and not any(ord(c)<32 for c in value)
            and not value.endswith((' ','.')))


def required_missing(metadata, basic=False):
    fields = ['url','video_id','title','description','author','author_url','duration','view_count','like_count','crawled_at_utc','files']
    if not basic:
        fields += ['resolution','width','height']
    missing = []
    for key in fields:
        value = metadata.get(key)
        if value is None or value == '' or isinstance(value,str) and not value.strip():
            missing.append(key)
        elif key in {'duration','width','height'} and (isinstance(value,bool) or not isinstance(value,(int,float)) or not 0<value<float('inf')):
            missing.append(key)
        elif key in {'view_count','like_count'} and (isinstance(value,bool) or not isinstance(value,int) or value<0):
            missing.append(key)
    return missing


def load_batch(path):
    batch = Path(path).resolve()
    manifest = json.loads((batch/'manifest.json').read_text(encoding='utf-8-sig'))
    if not isinstance(manifest.get('export_id'),str) or not re.fullmatch(r'[A-Za-z0-9_-]+',manifest['export_id']):
        raise ValueError('Unsafe export identity')
    if type(manifest.get('assigned_part')) is not int or manifest['assigned_part'] not in range(1,21):
        raise ValueError('Invalid batch number')
    for filename in ('tasks.jsonl','held_tasks.jsonl'):
        if sha256_file(batch/filename) != manifest[filename+'_sha256']:
            raise ValueError('Task list hash mismatch: '+filename)
    tasks = [json.loads(line) for line in (batch/'tasks.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    held = [json.loads(line) for line in (batch/'held_tasks.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    if len(tasks)!=manifest['ready_count'] or len(held)!=manifest['held_count']:
        raise ValueError('Batch task counts do not match manifest')
    ids, names = set(), set()
    for task in tasks+held:
        video_id, folder = task['video_id'], task['folder_name']
        key = unicodedata.normalize('NFC',folder).casefold()
        if not re.fullmatch(r'[A-Za-z0-9_-]{11}',video_id) or not safe_name(folder) or video_id in ids or key in names:
            raise ValueError('Duplicate/unsafe task identity or folder')
        if task.get('schema_version') not in (1,2) or task['assigned_part']!=manifest['assigned_part']:
            raise ValueError('Unsupported or misassigned task')
        if task.get('metadata_mode','full') not in {'full','basic','full_fallback'}:
            raise ValueError('Unsupported metadata stage')
        if task['url']!='https://www.youtube.com/watch?v='+video_id or task['output_filename']!='001.mkv':
            raise ValueError('Unexpected task URL/output filename')
        if task['format_policy'].get('format')!=FORMAT or task['format_policy'].get('format_sort')!=['res','fps']:
            raise ValueError('Unsupported format policy')
        if task['metadata_path']!='videos/'+folder+'/metadata.json':
            raise ValueError('folder_name and metadata_path differ')
        source = batch/task['metadata_path']
        if source.is_symlink() or source.parent.is_symlink() or not source.resolve().is_relative_to(batch):
            raise ValueError('Metadata path escapes batch')
        if sha256_file(source)!=task['metadata_sha256']:
            raise ValueError('Metadata hash mismatch: '+video_id)
        metadata = json.loads(source.read_text(encoding='utf-8-sig'))
        if metadata.get('video_id')!=video_id or metadata.get('url')!=task['url'] or metadata.get('files')!=['001.mkv']:
            raise ValueError('Metadata identity mismatch')
        missing = required_missing(metadata,task.get('metadata_mode')=='basic')
        if missing!=task['missing_fields']:
            raise ValueError('Metadata completeness differs from task contract')
        ids.add(video_id);names.add(key)
    if any(task['missing_fields'] or task.get('hold_reason') for task in tasks):
        raise ValueError('Held tasks must not be in tasks.jsonl')
    return batch, manifest, tasks
