CREATE {temp}TABLE IF NOT EXISTS {name} (
{owner}  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY ({keys}, node)
) STRICT, WITHOUT ROWID