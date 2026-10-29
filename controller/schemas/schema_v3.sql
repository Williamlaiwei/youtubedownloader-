ALTER TABLE videos ADD COLUMN metadata_mode TEXT NOT NULL DEFAULT 'full';
ALTER TABLE videos ADD COLUMN resolution_status TEXT NOT NULL DEFAULT 'unverified_metadata';
ALTER TABLE collected_metadata ADD COLUMN metadata_mode TEXT NOT NULL DEFAULT 'full';
ALTER TABLE collected_metadata ADD COLUMN resolution_status TEXT NOT NULL DEFAULT 'unverified_metadata';
PRAGMA user_version = 3;
