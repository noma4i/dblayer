CREATE TABLE IF NOT EXISTS transfer_outcome (
  transfer_id INTEGER NOT NULL,
  cycle_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('confirmed', 'rejected', 'failed', 'discarded')),
  revision INTEGER,
  value TEXT,
  terminal_at INTEGER NOT NULL,
  PRIMARY KEY (transfer_id, cycle_id)
) STRICT, WITHOUT ROWID