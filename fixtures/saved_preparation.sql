DROP TABLE sync_upload_prepare;
CREATE TABLE IF NOT EXISTS sync_upload_prepare (
  transfer_id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  mirror INTEGER NOT NULL,
  max_long_side INTEGER,
  min_frame_rate INTEGER,
  max_frame_rate INTEGER,
  source_bitrate_cap_percent INTEGER,
  audio_bitrate INTEGER,
  audio_channels INTEGER,
  audio_sample_rate INTEGER,
  recode_above_bytes INTEGER,
  max_width INTEGER,
  max_height INTEGER,
  enlarge INTEGER,
  crop_x REAL,
  crop_y REAL,
  crop_width REAL,
  crop_height REAL,
  output_width INTEGER,
  output_height INTEGER,
  rotation_degrees INTEGER,
  quality REAL,
  format TEXT,
  on_cannot_decode TEXT
) STRICT;
CREATE TABLE IF NOT EXISTS sync_upload_prepare_step (
  transfer_id INTEGER NOT NULL,
  from_long_side INTEGER NOT NULL,
  min_bps INTEGER NOT NULL,
  max_bps INTEGER NOT NULL
) STRICT;
INSERT INTO sync_upload_prepare (transfer_id, kind, mirror, max_long_side, min_frame_rate, max_frame_rate, source_bitrate_cap_percent, audio_bitrate, audio_channels, audio_sample_rate, recode_above_bytes, on_cannot_decode) SELECT transfer_id, 'video', 1, 1920, 30, 60, 95, 128000, 2, 44100, NULL, 'upload_source' FROM sync_upload;
INSERT INTO sync_upload_prepare_step SELECT transfer_id, 0, 500000, 900000 FROM sync_upload;
INSERT INTO sync_upload_prepare_step SELECT transfer_id, 720, 700000, 1200000 FROM sync_upload;
UPDATE manifest SET value = '1aabc63bd95c06e7a2f92ef114a6996366f22dd30eeabb529589491bcc4edda6' WHERE key = 'format_version';
