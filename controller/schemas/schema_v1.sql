CREATE TABLE import_runs (
    import_id TEXT PRIMARY KEY NOT NULL,
    started_at_utc TEXT NOT NULL,
    completed_at_utc TEXT,
    mode TEXT NOT NULL CHECK(mode = 'offline_existing'),
    project_root TEXT NOT NULL
);

CREATE TABLE source_files (
    source_id INTEGER PRIMARY KEY,
    relative_path TEXT NOT NULL,
    sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
    kind TEXT NOT NULL,
    snapshot_path TEXT NOT NULL,
    first_import_id TEXT NOT NULL REFERENCES import_runs(import_id),
    UNIQUE(relative_path, sha256)
);

CREATE TABLE run_sources (
    import_id TEXT NOT NULL REFERENCES import_runs(import_id),
    source_id INTEGER NOT NULL REFERENCES source_files(source_id),
    PRIMARY KEY(import_id, source_id)
);

CREATE TABLE videos (
    video_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY
        CHECK(length(video_id) = 11 AND video_id NOT GLOB '*[^A-Za-z0-9_-]*'),
    url TEXT NOT NULL CHECK(url = 'https://www.youtube.com/watch?v=' || video_id),
    original_title TEXT,
    folder_name TEXT,
    folder_name_key TEXT UNIQUE,
    naming_version TEXT,
    folder_locked_at TEXT,
    metadata_status TEXT NOT NULL DEFAULT 'pending' CHECK(metadata_status IN
        ('pending','partial','ready','restricted','permanent_failure','retry_wait',
         'rate_limited','insufficient','interrupted','error')),
    download_status TEXT NOT NULL DEFAULT 'not_started' CHECK(download_status IN
        ('not_started','completed_local','queued','running','completed','retry_wait','failed')),
    assigned_part INTEGER CHECK(assigned_part BETWEEN 1 AND 20),
    estimated_size_bytes INTEGER CHECK(estimated_size_bytes > 0),
    size_estimation_method TEXT,
    actual_size_bytes INTEGER CHECK(actual_size_bytes > 0),
    error_code TEXT,
    error_stage TEXT,
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
    current_metadata_version INTEGER REFERENCES metadata_versions(metadata_version_id),
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    CHECK((folder_name IS NULL) = (folder_name_key IS NULL)),
    CHECK(folder_name IS NULL OR length(folder_name) > 0)
);

CREATE TABLE source_lines (
    source_id INTEGER NOT NULL REFERENCES source_files(source_id),
    line_number INTEGER NOT NULL CHECK(line_number > 0),
    original_text TEXT NOT NULL,
    video_id TEXT REFERENCES videos(video_id),
    outcome TEXT NOT NULL CHECK(outcome IN ('valid','duplicate','invalid','blank')),
    reason TEXT,
    PRIMARY KEY(source_id, line_number)
);

CREATE TABLE metadata_versions (
    metadata_version_id INTEGER PRIMARY KEY,
    video_id TEXT NOT NULL REFERENCES videos(video_id),
    source_id INTEGER NOT NULL REFERENCES source_files(source_id),
    record_index INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('estimator_summary','customer_sample')),
    observed_at_utc TEXT NOT NULL,
    imported_status TEXT NOT NULL,
    metadata_status TEXT NOT NULL,
    error_code TEXT,
    missing_fields_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE(source_id, record_index)
);

CREATE TABLE attempts (
    attempt_id INTEGER PRIMARY KEY,
    video_id TEXT NOT NULL REFERENCES videos(video_id),
    source_id INTEGER NOT NULL REFERENCES source_files(source_id),
    record_index INTEGER NOT NULL,
    stage TEXT NOT NULL CHECK(stage IN ('metadata','download')),
    legacy_status TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    reported_attempt_count INTEGER CHECK(reported_attempt_count >= 0),
    observed_at_utc TEXT,
    payload_json TEXT NOT NULL,
    UNIQUE(source_id, record_index)
);

CREATE INDEX videos_metadata_status_idx ON videos(metadata_status);
CREATE INDEX videos_download_status_idx ON videos(download_status);
CREATE INDEX videos_assigned_part_idx ON videos(assigned_part);
CREATE INDEX source_lines_video_idx ON source_lines(video_id);
CREATE INDEX attempts_video_idx ON attempts(video_id, stage);
CREATE INDEX metadata_versions_video_idx ON metadata_versions(video_id);

CREATE TABLE local_media (
    local_media_id INTEGER PRIMARY KEY,
    video_id TEXT NOT NULL REFERENCES videos(video_id),
    relative_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK(size_bytes > 0),
    mtime_ns INTEGER NOT NULL,
    metadata_source_id INTEGER NOT NULL REFERENCES source_files(source_id),
    first_import_id TEXT NOT NULL REFERENCES import_runs(import_id),
    UNIQUE(relative_path, size_bytes, mtime_ns)
);

CREATE TRIGGER preserve_registered_folder
BEFORE UPDATE OF folder_name, folder_name_key ON videos
WHEN OLD.folder_name IS NOT NULL AND
    (NEW.folder_name IS NOT OLD.folder_name OR NEW.folder_name_key IS NOT OLD.folder_name_key)
BEGIN
    SELECT RAISE(ABORT, 'Registered folder names are immutable; use an explicit future migration');
END;

PRAGMA user_version = 1;
