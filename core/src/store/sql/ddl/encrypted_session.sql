CREATE TABLE IF NOT EXISTS encrypted_session (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  credential_digest TEXT NOT NULL,
  sequence INTEGER NOT NULL
) STRICT