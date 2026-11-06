CREATE TABLE IF NOT EXISTS op_part (
  seq INTEGER NOT NULL,
  item_position INTEGER NOT NULL,
  number INTEGER NOT NULL,
  etag TEXT NOT NULL,
  confirmed_bytes INTEGER NOT NULL,
  PRIMARY KEY (seq, item_position, number),
  FOREIGN KEY (seq, item_position) REFERENCES op_item(seq, position) ON DELETE CASCADE
) STRICT, WITHOUT ROWID