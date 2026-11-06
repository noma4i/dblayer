CREATE TABLE IF NOT EXISTS encrypted_answer (
  request_id INTEGER PRIMARY KEY,
  outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'http', 'too_large', 'unauthorized', 'revoked', 'session', 'security')),
  detail TEXT,
  operation_id INTEGER
) STRICT