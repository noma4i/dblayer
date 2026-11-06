CREATE TABLE IF NOT EXISTS op_effect (
  seq INTEGER NOT NULL,
  position INTEGER NOT NULL,
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  scope_key TEXT,
  kind TEXT NOT NULL CHECK (kind IN ('create', 'edit', 'destroy', 'join', 'leave')),
  revision INTEGER NOT NULL,
  PRIMARY KEY (seq, position)
) STRICT, WITHOUT ROWID