ALTER TABLE videos ADD COLUMN collected_metadata_id INTEGER REFERENCES collected_metadata(collection_id);
ALTER TABLE videos ADD COLUMN next_retry_at TEXT;
ALTER TABLE videos ADD COLUMN metadata_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(metadata_attempt_count >= 0);

CREATE TABLE collection_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    started_at_utc TEXT NOT NULL,
    heartbeat_at_utc TEXT NOT NULL,
    completed_at_utc TEXT,
    status TEXT NOT NULL,
    selected_count INTEGER NOT NULL,
    processed_count INTEGER NOT NULL DEFAULT 0,
    settings_json TEXT NOT NULL,
    message TEXT
);

CREATE TABLE collected_metadata (
    collection_id INTEGER PRIMARY KEY,
    video_id TEXT NOT NULL REFERENCES videos(video_id),
    run_id TEXT NOT NULL REFERENCES collection_runs(run_id),
    observed_at_utc TEXT NOT NULL,
    raw_path TEXT NOT NULL UNIQUE,
    raw_sha256 TEXT NOT NULL,
    customer_path TEXT NOT NULL UNIQUE,
    customer_sha256 TEXT NOT NULL,
    metadata_status TEXT NOT NULL,
    missing_fields_json TEXT NOT NULL,
    warnings_json TEXT NOT NULL,
    selected_format_ids TEXT,
    format_policy_version TEXT NOT NULL,
    estimated_size_bytes INTEGER,
    estimation_method TEXT
);

CREATE TABLE collection_attempts (
    id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES collection_runs(run_id),
    video_id TEXT NOT NULL REFERENCES videos(video_id),
    attempted_at_utc TEXT NOT NULL,
    outcome TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    retryable INTEGER NOT NULL CHECK(retryable IN (0,1))
);
CREATE INDEX collection_attempts_video_idx ON collection_attempts(video_id);
CREATE INDEX collected_metadata_video_idx ON collected_metadata(video_id);

CREATE TABLE controller_settings (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE exports (
    export_id TEXT PRIMARY KEY NOT NULL,
    created_at_utc TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('building','ready','preview','failed')),
    relative_path TEXT NOT NULL UNIQUE,
    selected_count INTEGER NOT NULL,
    excluded_count INTEGER NOT NULL,
    policy_json TEXT NOT NULL,
    manifest_sha256 TEXT,
    error_message TEXT
);

CREATE TABLE export_tasks (
    export_id TEXT NOT NULL REFERENCES exports(export_id),
    video_id TEXT NOT NULL REFERENCES videos(video_id),
    assigned_part INTEGER NOT NULL CHECK(assigned_part BETWEEN 1 AND 20),
    folder_name TEXT NOT NULL,
    task_json TEXT NOT NULL,
    metadata_sha256 TEXT NOT NULL,
    PRIMARY KEY(export_id, video_id)
);

PRAGMA user_version = 2;
