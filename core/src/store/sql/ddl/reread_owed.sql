CREATE TABLE IF NOT EXISTS reread_owed (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  relation TEXT NOT NULL,
  owed_at INTEGER NOT NULL,
  PRIMARY KEY (model, row_id, relation)
) STRICT, WITHOUT ROWID