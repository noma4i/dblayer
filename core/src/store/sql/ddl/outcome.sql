CREATE TABLE IF NOT EXISTS outcome (
  owner TEXT NOT NULL CHECK (owner IN ('operation', 'transfer')),
  id INTEGER NOT NULL,
  cycle_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('confirmed', 'rejected', 'failed', 'discarded')),
  revision INTEGER,
  value TEXT,
  model TEXT,
  row_id TEXT,
  terminal_at INTEGER NOT NULL,
  PRIMARY KEY (owner, id, cycle_id)
) STRICT, WITHOUT ROWID
