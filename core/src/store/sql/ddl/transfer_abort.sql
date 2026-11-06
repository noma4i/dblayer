CREATE TABLE IF NOT EXISTS transfer_abort (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_id INTEGER NOT NULL,
  item_id TEXT NOT NULL,
  storage_key TEXT NOT NULL,
  upload_id TEXT NOT NULL,
  abort_operation TEXT NOT NULL
) STRICT