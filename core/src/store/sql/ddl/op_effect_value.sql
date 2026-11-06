CREATE TABLE IF NOT EXISTS op_effect_value (
  seq INTEGER NOT NULL,
  position INTEGER NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (seq, position, node)
) STRICT, WITHOUT ROWID