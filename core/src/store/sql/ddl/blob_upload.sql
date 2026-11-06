CREATE TABLE IF NOT EXISTS blob_upload (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  seq INTEGER UNIQUE,
  consumer_seq INTEGER,
  generation INTEGER NOT NULL,
  created_at INTEGER,
  ready INTEGER NOT NULL DEFAULT 0,
  failed_at INTEGER,
  signed_id TEXT,
  total_bytes INTEGER NOT NULL DEFAULT 0,
  sent_bytes INTEGER NOT NULL DEFAULT 0,
  expired INTEGER NOT NULL DEFAULT 0 CHECK (expired IN (0, 1))
) STRICT