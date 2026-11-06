CREATE {temp}TABLE IF NOT EXISTS {table} (
  row_id TEXT NOT NULL,
  owner INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  value {value_type},
  PRIMARY KEY (row_id, owner, pos)
) STRICT, WITHOUT ROWID