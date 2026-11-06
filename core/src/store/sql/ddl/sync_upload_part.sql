CREATE TABLE IF NOT EXISTS sync_upload_part (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  action TEXT NOT NULL,
  number INTEGER NOT NULL,
  etag TEXT,
  PRIMARY KEY (model, row_id, action, number)
) STRICT, WITHOUT ROWID