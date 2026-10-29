"""Read-only 5+5 metadata experiment. Never starts/writes a Controller run."""
from collections import Counter
from datetime import datetime, timezone
import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import statistics
import sys
import time
from urllib.parse import urlsplit
import uuid

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
IDS = ['0CyRoGd4WG8', '2oX4EAy8aAs', 'Na5qnXEYUDE', 'DJGzS9xAiT4', 'LJOrH7pChHs']
LOCAL_KEYS = ['url', 'title', 'description', 'author', 'author_url', 'duration', 'view_count', 'like_count']
GROUPS = {'url': ['url'], 'title': ['title'], 'description': ['description'],
          'author': ['author', 'author_url'], 'duration': ['duration'],
          'view_count': ['view_count'], 'like_count': ['like_count']}
OVERRIDES = {'extractor_args': {'youtube': {'player_client': ['web'],
              'player_skip': ['js', 'configs'], 'skip': ['hls', 'dash', 'translated_subs']}},
             'ignore_no_formats_error': True}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    with path.open('x', encoding='utf-8') as handle:
        json.dump(value, handle, ensure_ascii=False, allow_nan=False, indent=2)
        handle.write('\n')


def production_state():
    path = ROOT / 'controller/data/controller.sqlite3'
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        run = dict(db.execute('SELECT run_id,status,processed_count,selected_count,heartbeat_at_utc,settings_json FROM collection_runs ORDER BY started_at_utc DESC LIMIT 1').fetchone())
        rows = db.execute('SELECT video_id,folder_name,assigned_part FROM videos').fetchall()
        return {'run': run, 'collected_count': db.execute('SELECT COUNT(*) FROM collected_metadata').fetchone()[0],
                'bindings': {r['video_id']: [r['folder_name'], r['assigned_part']] for r in rows}}
    finally:
        db.close()


def request_kind(url):
    parsed = urlsplit(url)
    path = parsed.path
    if path == '/watch':
        return 'watch_webpage'
    if '/youtubei/v1/player' in path:
        return 'player_api'
    if '/youtubei/v1/next' in path:
        return 'initial_data_api'
    if '/s/player/' in path or path.endswith('.js'):
        return 'player_javascript'
    if 'manifest' in path or 'manifest' in (parsed.hostname or '') or path.endswith(('.mpd', '.m3u8')):
        return 'stream_manifest'
    return 'other_config_or_metadata'


def present(value):
    return value is not None and value != '' and not (isinstance(value, str) and not value.strip())


def main():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, 'reconfigure'):
            stream.reconfigure(encoding='utf-8')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
    out = Path(__file__).resolve().parent / 'results' / stamp
    out.mkdir(parents=True, exist_ok=False)
    spec = importlib.util.spec_from_file_location('_benchmark_estimator', ROOT / 'estimate/youtube_size_estimator/estimate.py')
    e = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(e)
    before = production_state()
    code_paths = [ROOT / 'estimate/youtube_size_estimator/estimate.py'] + list((ROOT / 'controller/src/rmb_controller').glob('*.py'))
    code_hashes = {str(p.relative_to(ROOT)): digest(p.read_bytes()) for p in code_paths}
    save(out / 'experiment.json', {'started_at_utc': datetime.now(timezone.utc).isoformat(),
        'yt_dlp_version': e.YTDLP_VERSION, 'ids': IDS, 'lightweight_overrides': OVERRIDES,
        'lightweight_process': False, 'current_process': True, 'workers': 1, 'request_interval_seconds': 1,
        'timeout_seconds': 30, 'controller_before': {k:v for k,v in before.items() if k!='bindings'},
        'notes': ['The production Controller keeps running unchanged. This script opens its database read-only.',
                  'Alternating mode order for each paired video; fresh YoutubeDL instance per extraction.',
                  'Counts are yt-dlp urlopen calls, including failures; internal redirects are not separately counted.',
                  'Local completeness excludes resolution, which is explicitly deferred to VM.',
                  'Same process caches may warm; n=5 per mode is a small experiment, not a capacity forecast.']})
    results = []
    pacer = e.RequestPacer(1)

    class MeteredYDL(e.MetadataOnlyYDL):
        def __init__(self, options, pacer, requests):
            self.requests = requests
            super().__init__(options, pacer)

        def urlopen(self, request):
            url = request if isinstance(request, str) else request.url
            entry = {'category': request_kind(url), 'host': urlsplit(url).hostname,
                     'path': urlsplit(url).path, 'started_at_utc': datetime.now(timezone.utc).isoformat()}
            self.requests.append(entry)
            started = time.perf_counter()
            try:
                response = super().urlopen(request)
                entry['status'] = getattr(response, 'status', None)
                return response
            except Exception as exc:
                entry['error_type'] = type(exc).__name__
                raise
            finally:
                entry['elapsed_including_gate_seconds'] = time.perf_counter() - started

    for index, video_id in enumerate(IDS):
        modes = ['current', 'lightweight'] if index % 2 == 0 else ['lightweight', 'current']
        for mode in modes:
            logger = e.MetadataLogger(pacer)
            options = e.ydl_options(1, 30, logger)
            if mode == 'lightweight':
                options.update(OVERRIDES)
            requests = []
            started = time.perf_counter()
            record = {'video_id': video_id, 'mode': mode, 'success': False}
            try:
                with MeteredYDL(options, pacer, requests) as ydl:
                    extract_started = time.perf_counter()
                    info = ydl.extract_info(e.canonical_url(video_id), download=False, process=mode=='current')
                    record['extract_seconds'] = time.perf_counter() - extract_started
                    if not isinstance(info, dict) or info.get('id') != video_id:
                        raise ValueError('Unexpected video identity')
                    raw = ydl.sanitize_info(info)
                summary = e.metadata_row(video_id, info) if mode == 'current' else {}
                customer = {'url': e.canonical_url(video_id), 'title': raw.get('title'),
                            'description': raw.get('description'), 'author': raw.get('uploader') or raw.get('channel'),
                            'author_url': raw.get('uploader_url') or raw.get('channel_url'),
                            'duration': raw.get('duration'), 'view_count': raw.get('view_count'),
                            'like_count': raw.get('like_count'),
                            'resolution': summary.get('resolution') if mode=='current' else None}
                record.update({'success': True, 'fields': customer,
                    'local_field_presence': {key: present(customer[key]) for key in LOCAL_KEYS},
                    'local_group_presence': {key: all(present(customer[k]) for k in keys) for key,keys in GROUPS.items()},
                    'description_length': len(customer['description'] or ''),
                    'description_sha256': digest((customer['description'] or '').encode('utf-8')),
                    'format_count': len(raw.get('formats') or []),
                    'selected_format_ids': summary.get('selected_format_ids'),
                    'resolution_stage': 'selected_not_ffprobe_verified' if mode=='current' else 'vm_pending'})
                save(out / f'{video_id}-{mode}-raw.json', raw)
            except Exception as exc:
                record['error'] = e.exception_text(exc)
            record.update({'total_seconds': time.perf_counter() - started, 'request_count': len(requests),
                'requests_by_category': dict(Counter(r['category'] for r in requests)),
                'requests': requests, 'warnings': logger.messages})
            results.append(record)
            save(out / f'{video_id}-{mode}-measurement.json', record)
            print(f"{mode:11s} {video_id} {record['total_seconds']:.2f}s requests={len(requests)} success={record['success']}", flush=True)
            if pacer.stopped.is_set():
                raise RuntimeError('Experiment stopped after rate limit; production state was not changed')
            time.sleep(2)  # Small gap between test extractions; never changes the production pacer.
    aggregate = {}
    for mode in ('current', 'lightweight'):
        rows = [r for r in results if r['mode']==mode]
        successful = [r for r in rows if r['success']]
        aggregate[mode] = {'samples': len(rows), 'successes': len(successful),
            'total_seconds': sum(r['total_seconds'] for r in rows),
            'mean_seconds': statistics.mean(r['total_seconds'] for r in rows),
            'median_seconds': statistics.median(r['total_seconds'] for r in rows),
            'total_requests': sum(r['request_count'] for r in rows),
            'mean_requests': statistics.mean(r['request_count'] for r in rows),
            'requests_by_category': dict(sum((Counter(r['requests_by_category']) for r in rows), Counter())),
            'present_local_fields': sum(sum(r['local_field_presence'].values()) for r in successful),
            'expected_local_fields': len(rows)*len(LOCAL_KEYS),
            'present_local_groups': sum(sum(r['local_group_presence'].values()) for r in successful),
            'expected_local_groups': len(rows)*len(GROUPS),
            'all_local_groups_complete_videos': sum(all(r['local_group_presence'].values()) for r in successful)}
    pairs = []
    for video_id in IDS:
        a = next(r for r in results if r['video_id']==video_id and r['mode']=='current')
        b = next(r for r in results if r['video_id']==video_id and r['mode']=='lightweight')
        if a['success'] and b['success']:
            pairs.append({'video_id': video_id, 'description_identical': a['description_sha256']==b['description_sha256'],
                'description_length_current': a['description_length'], 'description_length_lightweight': b['description_length'],
                'equal_field_values': {key: a['fields'][key]==b['fields'][key] for key in LOCAL_KEYS},
                'current_seconds': a['total_seconds'], 'lightweight_seconds': b['total_seconds'],
                'current_requests': a['request_count'], 'lightweight_requests': b['request_count']})
    after = production_state()
    binding_changes = [vid for vid,old in before['bindings'].items() if
                      (old[0] is not None and after['bindings'][vid][0]!=old[0]) or after['bindings'][vid][1]!=old[1]]
    preservation = {'same_production_run': before['run']['run_id']==after['run']['run_id'],
        'production_run_before': before['run'], 'production_run_after': after['run'],
        'collected_count_before': before['collected_count'], 'collected_count_after': after['collected_count'],
        'existing_folder_or_batch_mapping_changes': binding_changes,
        'production_code_unchanged': all(digest((ROOT/p).read_bytes())==h for p,h in code_hashes.items())}
    summary = {'aggregate': aggregate, 'pairs': pairs, 'preservation': preservation,
        'speedup_mean': aggregate['current']['mean_seconds']/aggregate['lightweight']['mean_seconds'],
        'test_directory': str(out), 'completed_at_utc': datetime.now(timezone.utc).isoformat()}
    save(out / 'summary.json', summary)
    with (out / 'comparison.csv').open('x', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=['video_id','current_seconds','lightweight_seconds','current_requests','lightweight_requests','description_length_current','description_length_lightweight','description_identical'])
        writer.writeheader()
        writer.writerows({k:r[k] for k in writer.fieldnames} for r in pairs)
    lines = ['# Metadata 模式比較（隔離測試，未改正式流程）', '', '| 模式 | 成功 | 平均秒數 | 中位秒數 | 請求數 | 本機必要欄位 | 本機必要組 |', '|---|---:|---:|---:|---:|---:|---:|']
    for mode,a in aggregate.items():
        lines.append(f"| {mode} | {a['successes']}/{a['samples']} | {a['mean_seconds']:.3f} | {a['median_seconds']:.3f} | {a['total_requests']} | {a['present_local_fields']}/{a['expected_local_fields']} | {a['present_local_groups']}/{a['expected_local_groups']} |")
    lines += ['', '本機必要欄位排除解析度；作者名稱＋首頁算一組。完整描述以字元數及 SHA-256 與現有模式逐字比對。',
              '5 支影片已知可公開取得且跨語言。正式採集同期運行，共用 IP 的網路波動與快取會影響速度，不應直接外推整批耗時。',
              '請求數是 yt-dlp urlopen 次數，包含失敗；不另計 networking handler 的內部轉址。', '', '## 保留狀態', '', '```json', json.dumps(preservation, ensure_ascii=False, indent=2), '```']
    with (out / 'report.md').open('x', encoding='utf-8') as handle:
        handle.write('\n'.join(lines)+'\n')
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__=='__main__':
    main()
