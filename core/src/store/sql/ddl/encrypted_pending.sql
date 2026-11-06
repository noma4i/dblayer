CREATE TABLE IF NOT EXISTS encrypted_pending (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  credential_digest TEXT NOT NULL,
  request_id TEXT NOT NULL,
  response_key TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  path TEXT NOT NULL,
  body TEXT NOT NULL,
  owner_kind TEXT,
  owner_operation_id INTEGER,
  owner_item_id TEXT,
  owner_step TEXT,
  purpose INTEGER NOT NULL CHECK (purpose IN (0, 1)),
  req_id INTEGER NOT NULL
) STRICT