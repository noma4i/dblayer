CREATE TABLE IF NOT EXISTS scope_edge (
  model TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  row_id TEXT NOT NULL,
  name TEXT NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (model, scope_key, row_id, name)
) STRICT, WITHOUT ROWID