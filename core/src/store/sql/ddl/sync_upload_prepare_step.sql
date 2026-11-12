CREATE TABLE IF NOT EXISTS sync_upload_prepare_step (
  transfer_id INTEGER NOT NULL,
  from_long_side INTEGER NOT NULL,
  min_bps INTEGER NOT NULL,
  max_bps INTEGER NOT NULL
) STRICT