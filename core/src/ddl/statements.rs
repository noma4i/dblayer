pub(super) const CACHE_STATEMENTS: [&str; 18] = [
    "CREATE TABLE IF NOT EXISTS scope_member (
  model TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  row_id TEXT NOT NULL,
  order_key TEXT NOT NULL,
  place TEXT,
  PRIMARY KEY (model, scope_key, row_id)
) STRICT",
    "CREATE UNIQUE INDEX IF NOT EXISTS scope_walk ON scope_member (model, scope_key, order_key, row_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS scope_order_unique ON scope_member (model, scope_key, order_key)",
    "CREATE INDEX IF NOT EXISTS scope_member_row ON scope_member (model, row_id, scope_key)",
    "CREATE INDEX IF NOT EXISTS scope_member_place ON scope_member (model, scope_key, place, row_id) WHERE place IS NOT NULL",
    "CREATE TABLE IF NOT EXISTS scope_revision (
  model TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  row_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  operation_seq INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (model, scope_key, row_id)
) STRICT, WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS scope_revision_since ON scope_revision (model, scope_key, revision, row_id)",
    "CREATE TRIGGER IF NOT EXISTS scope_revision_insert AFTER INSERT ON scope_member BEGIN
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (new.model, new.scope_key, new.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
END",
    "CREATE TRIGGER IF NOT EXISTS scope_revision_delete AFTER DELETE ON scope_member BEGIN
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (old.model, old.scope_key, old.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
END",
    "CREATE TRIGGER IF NOT EXISTS scope_revision_move AFTER UPDATE OF order_key, place ON scope_member WHEN old.order_key IS NOT new.order_key OR old.place IS NOT new.place BEGIN
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (new.model, new.scope_key, new.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
END",
    "CREATE TRIGGER IF NOT EXISTS scope_revision_rename AFTER UPDATE OF model, scope_key, row_id ON scope_member WHEN old.model IS NOT new.model OR old.scope_key IS NOT new.scope_key OR old.row_id IS NOT new.row_id BEGIN
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (old.model, old.scope_key, old.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (new.model, new.scope_key, new.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
END",
    "CREATE TABLE IF NOT EXISTS scope_edge (
  model TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  row_id TEXT NOT NULL,
  name TEXT NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (model, scope_key, row_id, name)
) STRICT, WITHOUT ROWID",
    "CREATE TRIGGER IF NOT EXISTS scope_edge_member_delete AFTER DELETE ON scope_member BEGIN
  DELETE FROM scope_edge WHERE model = old.model AND scope_key = old.scope_key AND row_id = old.row_id;
END",
    "CREATE TRIGGER IF NOT EXISTS scope_edge_member_rename AFTER UPDATE OF row_id ON scope_member BEGIN
  UPDATE OR REPLACE scope_edge SET row_id = new.row_id WHERE model = old.model AND scope_key = old.scope_key AND row_id = old.row_id;
END",
    "CREATE TABLE IF NOT EXISTS tombstone (
  model TEXT NOT NULL,
  id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  deleted_at INTEGER NOT NULL,
  PRIMARY KEY (model, id)
) STRICT, WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS tombstone_age ON tombstone (deleted_at)",
    "CREATE TABLE IF NOT EXISTS query_window (
  key TEXT PRIMARY KEY NOT NULL,
  cursor TEXT,
  has_next INTEGER NOT NULL,
  total INTEGER,
  pages INTEGER NOT NULL,
  issued INTEGER NOT NULL,
  applied INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  landed_at INTEGER NOT NULL,
  stale_since INTEGER CHECK (stale_since > 0)
) STRICT",
    "CREATE TABLE IF NOT EXISTS window_marker (
  window_key TEXT NOT NULL,
  generation INTEGER NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence >= 0),
  field TEXT NOT NULL,
  value INTEGER NOT NULL,
  PRIMARY KEY (window_key, generation, field)
) STRICT, WITHOUT ROWID",
];

pub(super) const CORE_STATEMENTS: [&str; 48] = [
    "CREATE TABLE IF NOT EXISTS blob_upload (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  seq INTEGER UNIQUE,
  consumer_seq INTEGER,
  generation INTEGER NOT NULL,
  created_at INTEGER,
  ready INTEGER NOT NULL DEFAULT 0,
  failed_at INTEGER,
  signed_id TEXT,
  total_bytes INTEGER NOT NULL DEFAULT 0,
  sent_bytes INTEGER NOT NULL DEFAULT 0,
  expired INTEGER NOT NULL DEFAULT 0 CHECK (expired IN (0, 1))
) STRICT",
    "CREATE INDEX IF NOT EXISTS blob_expiry ON blob_upload (consumer_seq, expired, ready, created_at)",
    "CREATE INDEX IF NOT EXISTS blob_failed_expiry ON blob_upload (consumer_seq, expired, failed_at)",
    "CREATE TABLE IF NOT EXISTS blob_use (
  consumer_seq INTEGER NOT NULL,
  parameter TEXT NOT NULL,
  blob_id TEXT NOT NULL,
  PRIMARY KEY (consumer_seq, parameter)
) STRICT, WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS blob_consumer ON blob_use (blob_id)",
    "CREATE TABLE IF NOT EXISTS manifest (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT NOT NULL
) STRICT",
    "CREATE TABLE IF NOT EXISTS op (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  action_key TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'failed', 'delivery_unknown', 'committed', 'rolledback')),
  temp_id TEXT,
  transport_started INTEGER NOT NULL DEFAULT 0,
  base_revision INTEGER,
  created_at INTEGER NOT NULL,
  dedupe_key TEXT,
  client_mutation_id TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))),
  attempts INTEGER NOT NULL DEFAULT 0,
  since INTEGER NOT NULL,
  error_kind TEXT,
  error_message TEXT
) STRICT",
    "CREATE TABLE IF NOT EXISTS op_effect (
  seq INTEGER NOT NULL,
  position INTEGER NOT NULL,
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  scope_key TEXT,
  kind TEXT NOT NULL CHECK (kind IN ('create', 'edit', 'destroy', 'join', 'leave')),
  revision INTEGER NOT NULL,
  PRIMARY KEY (seq, position)
) STRICT, WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS op_effect_row ON op_effect (model, row_id, seq, position)",
    "CREATE INDEX IF NOT EXISTS op_effect_scope ON op_effect (model, scope_key, seq, position, row_id) WHERE scope_key IS NOT NULL",
    "CREATE TABLE IF NOT EXISTS op_effect_value (
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
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS op_item (
  seq INTEGER NOT NULL REFERENCES op(seq) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  transfer_operation_id INTEGER NOT NULL,
  client_key TEXT NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16)))),
  row_id TEXT,
  file_id TEXT,
  source_error TEXT,
  kind TEXT,
  body TEXT,
  phase TEXT NOT NULL,
  stage TEXT NOT NULL,
  scenario TEXT,
  total_bytes INTEGER NOT NULL,
  sent_bytes INTEGER NOT NULL,
  error_kind TEXT,
  error_message TEXT,
  storage_key TEXT,
  upload_id TEXT,
  signed_id TEXT,
  blob_id TEXT,
  document TEXT,
  operation TEXT,
  transferred INTEGER NOT NULL,
  confirmed_bytes INTEGER NOT NULL,
  checksum TEXT,
  put_url TEXT,
  url_issued_at INTEGER,
  failed_at INTEGER,
  background_refused TEXT,
  offline INTEGER,
  PRIMARY KEY (seq, position)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS op_item_retry (
  seq INTEGER NOT NULL,
  position INTEGER NOT NULL,
  stage TEXT NOT NULL,
  part_number INTEGER NOT NULL,
  attempts INTEGER NOT NULL,
  retry_at INTEGER,
  PRIMARY KEY (seq, position, stage, part_number),
  FOREIGN KEY (seq, position) REFERENCES op_item(seq, position) ON DELETE CASCADE
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS op_item_header (
  seq INTEGER NOT NULL,
  position INTEGER NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (seq, position, name),
  FOREIGN KEY (seq, position) REFERENCES op_item(seq, position) ON DELETE CASCADE
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS op_row (
  seq INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  row_id TEXT NOT NULL,
  PRIMARY KEY (seq, pos)
) STRICT, WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS op_row_id ON op_row (row_id)",
    "CREATE TABLE IF NOT EXISTS transfer_abort (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_id INTEGER NOT NULL,
  item_id TEXT NOT NULL,
  storage_key TEXT NOT NULL,
  upload_id TEXT NOT NULL,
  abort_operation TEXT NOT NULL
) STRICT",
    "CREATE UNIQUE INDEX IF NOT EXISTS transfer_abort_identity ON transfer_abort (operation_id, item_id)",
    "CREATE TABLE IF NOT EXISTS op_part (
  seq INTEGER NOT NULL,
  item_position INTEGER NOT NULL,
  number INTEGER NOT NULL,
  etag TEXT NOT NULL,
  confirmed_bytes INTEGER NOT NULL,
  PRIMARY KEY (seq, item_position, number),
  FOREIGN KEY (seq, item_position) REFERENCES op_item(seq, position) ON DELETE CASCADE
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS op_value (
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
) STRICT, WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS op_prior_row ON op_value (name, text, seq, parent) WHERE item_position = -1 AND part = 'prior'",
    "CREATE INDEX IF NOT EXISTS op_reference_text ON op_value (text, seq) WHERE text IS NOT NULL",
    "CREATE TABLE IF NOT EXISTS row_sync (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'sending', 'failed', 'delivery_unknown')),
  error_kind TEXT,
  error_message TEXT,
  attempts INTEGER NOT NULL,
  since INTEGER NOT NULL,
  PRIMARY KEY (model, row_id)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS sync_upload (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  action TEXT NOT NULL,
  status TEXT NOT NULL,
  stage TEXT NOT NULL,
  sent INTEGER NOT NULL,
  total INTEGER NOT NULL,
  wait TEXT,
  attempts INTEGER NOT NULL,
  error_kind TEXT,
  error_message TEXT,
  seq INTEGER,
  source TEXT,
  batch INTEGER,
  upload_id TEXT,
  completed INTEGER,
  created_at INTEGER,
  inline INTEGER,
  size INTEGER,
  sha256 TEXT,
  mutation_path TEXT,
  mutation_body TEXT,
  on_failure TEXT,
  steps_path TEXT,
  steps_body TEXT,
  transfer_id INTEGER,
  cycle_id INTEGER,
  signed_id TEXT,
  aborted INTEGER,
  op_id INTEGER,
  fields TEXT,
  PRIMARY KEY (model, row_id, action)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS blob_cache (
  content_sha256 TEXT NOT NULL PRIMARY KEY,
  blob_id TEXT NOT NULL,
  created_at INTEGER NOT NULL
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS sync_upload_batch (
  batch_id INTEGER NOT NULL PRIMARY KEY,
  joint TEXT NOT NULL
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS upload_orphan (
  upload_id TEXT NOT NULL PRIMARY KEY,
  key TEXT NOT NULL
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS sync_upload_part (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  action TEXT NOT NULL,
  number INTEGER NOT NULL,
  etag TEXT,
  PRIMARY KEY (model, row_id, action, number)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS sync_action (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT NOT NULL,
  PRIMARY KEY (model, row_id, pos)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS sync_field (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT NOT NULL,
  PRIMARY KEY (model, row_id, pos)
) STRICT, WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS op_open ON op (action_key, status)",
    "CREATE INDEX IF NOT EXISTS op_temp ON op (temp_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS op_open_dedupe ON op (action_key, dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'delivery_unknown')",
    "CREATE TABLE IF NOT EXISTS op_error (
  seq INTEGER NOT NULL,
  item_position INTEGER NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (seq, item_position, node)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS sync_row_error (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (model, row_id, node)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS sync_upload_error (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  action TEXT NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (model, row_id, action, node)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS quarantine (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  model TEXT,
  reason TEXT NOT NULL
) STRICT",
    "CREATE TABLE IF NOT EXISTS quarantine_value (
  seq INTEGER NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (seq, node)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS reset_intent (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  mode TEXT NOT NULL,
  started_at INTEGER NOT NULL
) STRICT",
    "CREATE TABLE IF NOT EXISTS reset_intent_model (
  pos INTEGER PRIMARY KEY,
  model TEXT NOT NULL
) STRICT",
    "CREATE INDEX IF NOT EXISTS quarantine_reason ON quarantine (reason, seq)",
    "CREATE TABLE IF NOT EXISTS encrypted_session (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  credential_digest TEXT NOT NULL,
  sequence INTEGER NOT NULL
) STRICT",
    "CREATE TABLE IF NOT EXISTS encrypted_pending (
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
) STRICT",
    "CREATE TABLE IF NOT EXISTS encrypted_answer (
  request_id INTEGER PRIMARY KEY,
  outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'http', 'too_large', 'unauthorized', 'revoked', 'session')),
  detail TEXT,
  operation_id INTEGER
) STRICT",
    "CREATE TABLE IF NOT EXISTS restore_failed (
  source_table TEXT NOT NULL,
  key TEXT NOT NULL,
  reason TEXT NOT NULL,
  columns TEXT NOT NULL,
  PRIMARY KEY (source_table, key)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS file (
  id TEXT PRIMARY KEY NOT NULL,
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  field TEXT NOT NULL,
  name TEXT NOT NULL,
  mime TEXT NOT NULL,
  size INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  location TEXT NOT NULL CHECK (location IN ('inline', 'disk')),
  data BLOB,
  created_at INTEGER NOT NULL
) STRICT",
    "CREATE UNIQUE INDEX IF NOT EXISTS file_pair ON file (model, row_id, field)",
    "CREATE TABLE IF NOT EXISTS local_pending (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  path TEXT NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (model, row_id, path, node)
) STRICT, WITHOUT ROWID",
];

