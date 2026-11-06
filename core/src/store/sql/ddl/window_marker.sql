CREATE TABLE IF NOT EXISTS window_marker (
  window_key TEXT NOT NULL,
  generation INTEGER NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence >= 0),
  field TEXT NOT NULL,
  value INTEGER NOT NULL,
  PRIMARY KEY (window_key, generation, field)
) STRICT, WITHOUT ROWID