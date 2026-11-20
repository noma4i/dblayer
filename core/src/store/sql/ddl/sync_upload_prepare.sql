CREATE TABLE IF NOT EXISTS sync_upload_prepare (
  transfer_id INTEGER PRIMARY KEY,
  descriptor BLOB NOT NULL
) STRICT