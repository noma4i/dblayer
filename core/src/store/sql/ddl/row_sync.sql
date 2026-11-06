CREATE TABLE IF NOT EXISTS row_sync (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'sending', 'failed')),
  error_kind TEXT,
  error_message TEXT,
  attempts INTEGER NOT NULL,
  since INTEGER NOT NULL,
  PRIMARY KEY (model, row_id)
) STRICT, WITHOUT ROWID