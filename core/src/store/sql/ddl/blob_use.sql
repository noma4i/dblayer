CREATE TABLE IF NOT EXISTS blob_use (
  consumer_seq INTEGER NOT NULL,
  parameter TEXT NOT NULL,
  blob_id TEXT NOT NULL,
  PRIMARY KEY (consumer_seq, parameter)
) STRICT, WITHOUT ROWID