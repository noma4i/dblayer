CREATE TABLE IF NOT EXISTS sync_upload_error (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  action TEXT NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (model, row_id, action, node)
) STRICT, WITHOUT ROWID