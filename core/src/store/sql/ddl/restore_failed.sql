CREATE TABLE IF NOT EXISTS restore_failed (
  source_table TEXT NOT NULL,
  key TEXT NOT NULL,
  reason TEXT NOT NULL,
  columns TEXT NOT NULL,
  PRIMARY KEY (source_table, key)
) STRICT, WITHOUT ROWID