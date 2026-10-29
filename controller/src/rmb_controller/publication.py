"""Verified archives and updates to one permanent set of task directories."""
import json
import os
from pathlib import Path
import re
import uuid
import zipfile
import time

from .artifacts import json_bytes, sha256


def checked_files(root):
    root = Path(root).resolve()
    files = []
    for path in root.rglob('*'):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError(f'Unsafe publication path: {path}')
        if path.is_file():
            files.append(path)
    return files


def verified_archive(root, destination, files=None):
    """Verify every archived byte before any caller removes an expanded copy."""
    root, destination = Path(root).resolve(), Path(destination).resolve()
    files = checked_files(root) if files is None else list(files)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + '.tmp-' + uuid.uuid4().hex[:8])
    hashes = {}
    with zipfile.ZipFile(temporary, 'x', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(files):
            if path.is_symlink() or not path.resolve().is_relative_to(root):
                raise ValueError('Archive input escaped its source')
            relative = path.relative_to(root).as_posix()
            payload = path.read_bytes()
            hashes[relative] = sha256(payload)
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, payload)
    with zipfile.ZipFile(temporary) as archive:
        if set(archive.namelist()) != set(hashes):
            raise ValueError('Archive file inventory mismatch')
        for relative, digest in hashes.items():
            if sha256(archive.read(relative)) != digest:
                raise ValueError('Archive verification failed')
    if destination.exists():
        raise ValueError('Refusing to overwrite an existing archive')
    temporary.rename(destination)
    return hashes


def remove_archived_tree(path, root, hashes):
    """Only remove files whose exact content is already in a verified archive."""
    root, path = Path(root).resolve(), Path(path).resolve()
    if path == root or not path.is_relative_to(root):
        raise ValueError('Cleanup target escaped the named archive source')
    files = checked_files(path)
    for item in files:
        if hashes.get(item.relative_to(root).as_posix()) != sha256(item.read_bytes()):
            raise ValueError('Cleanup source changed after archival')
    for item in files:
        item.unlink()
    for directory in sorted((p for p in path.rglob('*') if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        directory.rmdir()
    path.rmdir()


def compact_snapshot(staging, manifest):
    """Keep just the 20 batch directories and documents inside VMtask."""
    staging = Path(staging)
    distribution = staging.parent / 'VMpackages'
    distribution.mkdir()
    hashes = manifest['record_hashes']
    for name in ('worker', 'packages', 'vm_worker_bundle.tar.gz'):
        (staging / name).rename(distribution / name)
    package_hashes = {key: hashes.pop(key) for key in list(hashes)
                      if key.startswith(('worker/', 'packages/')) or key == 'vm_worker_bundle.tar.gz'}
    manifest['distribution_hashes'] = package_hashes
    manifest['distribution_directory'] = '../VMpackages'
    for directory, prefix in (('bad_files', 'bad_files_'), ('records', '')):
        for path in list((staging / directory).iterdir()):
            if path.is_dir():
                continue
            old = path.relative_to(staging).as_posix()
            new = prefix + path.name
            path.rename(staging / new)
            if old in hashes:
                hashes[new] = hashes.pop(old)
    sources = staging / 'records/sources'
    archive_path = staging / 'source_records.zip'
    if sources.exists():
        archived = verified_archive(sources, archive_path)
        # The source ZIP uses paths relative to records/sources.
        remove_archived_tree(sources, staging, {'records/sources/' + k: v for k, v in archived.items()})
    else:
        with zipfile.ZipFile(archive_path, 'x'):
            pass
    for key in list(hashes):
        if key.startswith('records/sources/'):
            del hashes[key]
    hashes['source_records.zip'] = sha256(archive_path.read_bytes())
    source_manifest = staging / 'source_manifest.json'
    sources_index = json.loads(source_manifest.read_text(encoding='utf-8'))
    for entry in sources_index:
        entry['archive_member'] = Path(entry['copied_path']).name
        entry['copied_path'] = 'source_records.zip'
    source_manifest.write_bytes(json_bytes(sources_index))
    hashes['source_manifest.json'] = sha256(source_manifest.read_bytes())
    for directory in ('bad_files', 'records'):
        (staging / directory).rmdir()
    manifest['layout_version'] = 2
    return distribution


def atomic_file(source, target):
    payload = source.read_bytes()
    if target.is_file() and target.read_bytes() == payload:
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / ('.update-' + uuid.uuid4().hex[:8])
    with temporary.open('xb') as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    # Windows virus/index scanners may briefly keep a newly read archive open.
    for attempt in range(6):
        try:
            os.replace(temporary, target)
            break
        except PermissionError:
            if attempt == 5:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(.1 * (attempt + 1))
    return True


def ensure_writable(paths):
    """Refuse up front when another program (Excel, WPS...) holds a file open; Windows then blocks replacing it."""
    locked = []
    for path in paths:
        try:
            with Path(path).open('ab'):
                pass
        except PermissionError:
            locked.append(Path(path).name)
    if locked:
        raise ValueError('VMtask 未更新（維持上一版，未改動任何檔案）：下列檔案正被其他程式開啟，'
                         '請先關閉（例如 Excel／WPS）再整理：' + '、'.join(locked))


def merge_snapshot(root, staging, archive_dir, export_id, before_publish=None):
    """Never rename a batch: retain its directory and identical metadata files."""
    root, staging, archive_dir = Path(root), Path(staging), Path(archive_dir)
    stage_files = checked_files(staging)
    desired = {p.relative_to(staging).as_posix(): p for p in stage_files}
    previous = checked_files(root)
    for path in previous:
        relative = path.relative_to(root).as_posix()
        if relative not in desired and relative not in {'legacy_sample_cleanup.jsonl'}:
            raise ValueError(f'Existing file is absent from new snapshot; refusing removal: {relative}')
    changed = [p for p in previous if p.relative_to(root).as_posix() in desired
               and p.read_bytes() != desired[p.relative_to(root).as_posix()].read_bytes()]
    ensure_writable(changed)  # before the journal, READY removal or any archive: a locked file changes nothing
    archive = archive_dir / (export_id + '-replaced-files.zip')
    if changed:
        verified_archive(root, archive, changed)
    journal = archive_dir / 'pending_publication.json'
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.write_bytes(json_bytes({'root': str(root.resolve()), 'export_id': export_id,
                                    'staging': str(staging.resolve()), 'backup': str(archive)}))
    # An interrupted update never retains a misleading READY marker.
    (root / 'READY.json').unlink(missing_ok=True)
    if before_publish:
        before_publish()
    for part in range(1, 21):
        (root / f'batch-{part:02d}' / 'videos').mkdir(parents=True, exist_ok=True)
    for source in sorted(stage_files, key=lambda p: (p.name in {'manifest.json', 'READY.json'}, p.name == 'READY.json')):
        atomic_file(source, root / source.relative_to(staging))
    return archive if changed else None


def merge_distribution(staging, destination):
    destination = Path(destination)
    destination.mkdir(exist_ok=True)
    for path in checked_files(staging):
        atomic_file(path, destination / path.relative_to(staging))


STAGE_NAME = 'vmtask_stage'
LEGACY_STAGE = re.compile(r'\.vm-stage-[0-9a-f]{8}')


def discard_staging(staging, parent):
    """Only disposable, generated staging outside the public task directory."""
    staging, parent = Path(staging).resolve(), Path(parent).resolve()
    if staging.parent != parent or not (staging.name == STAGE_NAME or LEGACY_STAGE.fullmatch(staging.name)):
        raise ValueError('Invalid staging cleanup boundary')
    for path in checked_files(staging):
        path.unlink()
    for directory in sorted((p for p in staging.rglob('*') if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        directory.rmdir()
    staging.rmdir()


def clear_staging(data_dir, project_root):
    """Staging is rebuilt from SQLite on every export, so leftovers (including older versions') are disposable."""
    data_dir, project_root = Path(data_dir), Path(project_root)
    for leftover, parent in [(data_dir / STAGE_NAME, data_dir)] + [(p, project_root) for p in project_root.glob('.vm-stage-*')]:
        if leftover.is_dir() and not leftover.is_symlink() and (leftover.name == STAGE_NAME or LEGACY_STAGE.fullmatch(leftover.name)):
            discard_staging(leftover, parent)
