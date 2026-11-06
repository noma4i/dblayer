CREATE TABLE IF NOT EXISTS op_item_header (
  seq INTEGER NOT NULL,
  position INTEGER NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (seq, position, name),
  FOREIGN KEY (seq, position) REFERENCES op_item(seq, position) ON DELETE CASCADE
) STRICT, WITHOUT ROWID