"""Reuse the project's proven metadata-only extractor without changing its files."""
import importlib.util
from pathlib import Path
import sys
import time

from .validation import classify_failure, missing_customer_fields

FORMAT_POLICY = {
    "version": "highest-available-v1", "format": "bestvideo*+bestaudio/best",
    "format_sort": ["res", "fps"], "format_sort_force": True,
    "merge_output_format": "mkv", "output_filename": "001.mkv",
}

BASIC_EXTRACTOR_ARGS = {'youtube': {'player_client': ['web'],
    'player_skip': ['js', 'configs'], 'skip': ['hls', 'dash', 'translated_subs']}}


class YouTubeAdapter:
    def __init__(self, project_root, interval=1.0, timeout=30.0, mode='basic'):
        if interval < 1 or timeout <= 0:
            raise ValueError("Request interval must be >=1 second; timeout must be positive")
        path = Path(project_root) / "estimate/youtube_size_estimator/estimate.py"
        name = "_rmb_existing_estimator"
        specification = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(specification)
        sys.modules[name] = module
        try:
            specification.loader.exec_module(module)
        except ModuleNotFoundError as exc:
            raise ValueError("Metadata collection needs the existing estimate/youtube_size_estimator/.venv Python") from exc
        self.module = module
        self.interval, self.timeout = interval, timeout
        self.pacer = module.RequestPacer(interval)
        if mode not in {'basic', 'full'}:
            raise ValueError('Unknown metadata mode')
        self.mode = mode

    def set_interval(self, interval):
        """Change the shared request gap mid-run; takes effect for requests scheduled from now on."""
        if interval < 1:
            raise ValueError("Request interval must be >=1 second")
        self.interval = self.pacer.interval = float(interval)
        # Do not wait out a longer gap already booked; deliberately no lock, as waiters sleep while holding it.
        self.pacer.next_request = min(self.pacer.next_request, time.monotonic() + self.interval)

    def stop(self):
        self.pacer.stop()

    def pause(self, seconds):
        self.pacer.pause(seconds)

    def _extract_mode(self, video_id, mode):
        logger = self.module.MetadataLogger(self.pacer)
        try:
            options = self.module.ydl_options(self.interval, self.timeout, logger)
            if mode == 'basic':
                options.update(extractor_args=BASIC_EXTRACTOR_ARGS, ignore_no_formats_error=True)
            with self.module.MetadataOnlyYDL(options, self.pacer) as ydl:
                statuses = []
                if mode == 'basic':
                    extractor = ydl.get_info_extractor('Youtube')
                    original = extractor._extract_player_responses
                    def capture(*args, **kwargs):
                        responses, player_url = original(*args, **kwargs)
                        statuses.extend(r.get('playabilityStatus', {}) for r in responses)
                        return responses, player_url
                    extractor._extract_player_responses = capture
                info = ydl.extract_info(self.module.canonical_url(video_id), download=False, process=mode!='basic')
                if not isinstance(info, dict) or info.get("id") != video_id or info.get("_type", "video") != "video":
                    raise ValueError("Extractor did not return the requested single video")
                if statuses and not any(s.get('status') == 'OK' for s in statuses):
                    reasons = [s.get('reason') or s.get('status') for s in statuses if s.get('status') not in (None, 'OK')]
                    if reasons:
                        raise RuntimeError(' | '.join(reasons))
                # sanitize_info is the yt-dlp JSON serialization boundary; keep all public info.
                raw = ydl.sanitize_info(info)
            summary = self.module.metadata_row(video_id, info) if mode == 'full' else {}
            return {"raw": raw, "summary": summary, "warnings": logger.messages,
                    "metadata_mode": mode, "playability_statuses": statuses}
        except self.module.BatchStopped:
            raise InterruptedError("Collection batch stopped")
        except Exception as exc:
            message = self.module.exception_text(exc)
            if logger.messages:
                message += " | " + " | ".join(logger.messages)
            raise RuntimeError(message) from exc

    def extract(self, video_id):
        result = self._extract_mode(video_id, self.mode)
        if self.mode != 'basic':
            return result
        raw = result['raw']
        basic = {'url': self.module.canonical_url(video_id), 'video_id': video_id, 'title': raw.get('title'),
                 'description': raw.get('description'), 'author': raw.get('uploader') or raw.get('channel'),
                 'author_url': raw.get('uploader_url') or raw.get('channel_url'), 'duration': raw.get('duration'),
                 'view_count': raw.get('view_count'), 'like_count': raw.get('like_count'),
                 'crawled_at_utc': 'pending-save', 'files': ['001.mkv']}
        missing = missing_customer_fields(basic, resolution_pending=True)
        if not missing:
            return result
        try:
            full = self._extract_mode(video_id, 'full')
        except RuntimeError as exc:
            failure = classify_failure(str(exc))
            if failure.status in {'restricted','permanent_failure','rate_limited'}:
                raise
            result['warnings'].append('Full fallback unavailable; basic response preserved: '+str(exc))
            return result
        full['metadata_mode'] = 'full_fallback'
        full['basic_raw'] = raw
        full['warnings'] = result['warnings'] + full['warnings'] + ['Basic metadata fallback: ' + ', '.join(missing)]
        return full
