CREATE TABLE IF NOT EXISTS op_value (
  seq INTEGER NOT NULL,
  item_position INTEGER NOT NULL DEFAULT -1,
  part TEXT NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (seq, item_position, part, node)
) STRICT, WITHOUT ROWID