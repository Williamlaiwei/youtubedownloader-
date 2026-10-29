"""SQLite initialization; all import mutations use one caller-owned transaction."""
from pathlib import Path
import sqlite3
from datetime import datetime, timezone
import uuid

SCHEMA_VERSION = 3


def backup_database(connection, path, target_version):
    directory = path.parent / 'backups'
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    backup_path = directory / f'before-schema-v{target_version}-{stamp}-{uuid.uuid4().hex[:8]}.sqlite3'
    with backup_path.open('xb'):
        pass
    backup = sqlite3.connect(backup_path)
    try:
        connection.backup(backup)
    finally:
        backup.close()


def connect_database(path, *, readonly=False):
    path = Path(path).resolve()
    if readonly:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=30)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        initialized = version == 0
        if version == 0 and not readonly:
            if connection.execute("SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchone():
                raise ValueError("Refusing to initialize an unrecognized database")
            schema = Path(__file__).resolve().parents[2] / "schemas" / "schema_v1.sql"
            connection.executescript("BEGIN IMMEDIATE;\n" + schema.read_text(encoding="utf-8") + "\nCOMMIT;")
            version = 1
        if version == 1 and not readonly:
            if not initialized:
                backup_dir = path.parent / "backups"
                backup_dir.mkdir(parents=True, exist_ok=True)
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
                backup_path = backup_dir / f"before-schema-v2-{stamp}-{uuid.uuid4().hex[:8]}.sqlite3"
                with backup_path.open("xb"):
                    pass
                backup = sqlite3.connect(backup_path)
                try:
                    connection.backup(backup)
                finally:
                    backup.close()
            migration = Path(__file__).resolve().parents[2] / "schemas" / "schema_v2.sql"
            connection.executescript("BEGIN IMMEDIATE;\n" + migration.read_text(encoding="utf-8") + "\nCOMMIT;")
            version = 2
        if version == 2 and not readonly:
            if not initialized:
                backup_database(connection, path, 3)
            migration = Path(__file__).resolve().parents[2] / 'schemas' / 'schema_v3.sql'
            connection.executescript('BEGIN IMMEDIATE;\n' + migration.read_text(encoding='utf-8') + '\nCOMMIT;')
            version = 3
        if version != SCHEMA_VERSION:
            raise ValueError(f"Unsupported database schema version: {version} "
                             f"(supported: {SCHEMA_VERSION}; database: {path})")
        return connection
    except BaseException:
        connection.close()
        raise
