"""Immutable artifacts and an OS lock released automatically after a crash."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n").encode("utf-8")


def write_new(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    return sha256(data)


def verified_read(data_dir, relative_path, expected_hash):
    root = Path(data_dir).resolve()
    target = root / relative_path
    if not target.resolve().is_relative_to(root):
        raise ValueError(f"Artifact resolves outside data directory: {relative_path}")
    data = target.read_bytes()
    if sha256(data) != expected_hash:
        raise ValueError(f"Artifact hash mismatch: {relative_path}")
    return data


@contextmanager
def controller_lock(data_dir):
    path = Path(data_dir) / ".controller.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ValueError("Another Controller writer is running; use status or stop") from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
