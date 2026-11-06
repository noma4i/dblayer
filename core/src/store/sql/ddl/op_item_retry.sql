CREATE TABLE IF NOT EXISTS op_item_retry (
  seq INTEGER NOT NULL,
  position INTEGER NOT NULL,
  stage TEXT NOT NULL,
  part_number INTEGER NOT NULL,
  attempts INTEGER NOT NULL,
  retry_at INTEGER,
  PRIMARY KEY (seq, position, stage, part_number),
  FOREIGN KEY (seq, position) REFERENCES op_item(seq, position) ON DELETE CASCADE
) STRICT, WITHOUT ROWID