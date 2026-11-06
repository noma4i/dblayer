CREATE TABLE IF NOT EXISTS scope_revision (
  model TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  row_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  operation_seq INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (model, scope_key, row_id)
) STRICT, WITHOUT ROWID