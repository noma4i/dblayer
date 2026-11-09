CREATE TABLE IF NOT EXISTS query_window (
  key TEXT PRIMARY KEY NOT NULL,
  cursor TEXT,
  anchor TEXT,
  has_next INTEGER NOT NULL,
  total INTEGER,
  pages INTEGER NOT NULL,
  issued INTEGER NOT NULL,
  applied INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  landed_at INTEGER NOT NULL,
  stale_since INTEGER CHECK (stale_since > 0)
) STRICT