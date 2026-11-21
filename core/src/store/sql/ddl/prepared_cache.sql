CREATE TABLE IF NOT EXISTS prepared_cache (
  source_sha256 TEXT NOT NULL,
  prepare_sha256 TEXT NOT NULL,
  file TEXT NOT NULL,
  size INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  name TEXT,
  mime TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (source_sha256, prepare_sha256)
) STRICT, WITHOUT ROWID
