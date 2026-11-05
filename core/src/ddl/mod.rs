use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::schema::{AssociationSpec, FieldKind, FieldSpec, Membership, ModelSpec, Sort, Storage};

mod layout;
pub use layout::*;
use crate::value::ScalarKind;

pub const SCHEMA_VERSION: i64 = 2;

pub const FORMAT_VERSION_KEY: &str = "format_version";
pub const DATA_VERSION_KEY: &str = "data_version";
pub const REVISION_SEQ_KEY: &str = "revision_seq";
pub(crate) const CARRIED_MANIFEST_KEYS: [&str; 3] = ["saved_epoch", "saved_generation", REVISION_SEQ_KEY];
/// The tables a format change moves into the new layout (`store_boot_format_change`).
pub(crate) const CARRIED_TABLES: [&str; 19] = ["op", "op_item", "op_item_retry", "op_item_header", "op_row", "op_part", "op_value", "op_error", "transfer_abort", "blob_upload", "blob_use", "encrypted_session", "encrypted_pending", "encrypted_answer", "sync_upload", "sync_upload_error", "local_pending", "file", "restore_failed"];

pub const SERVICE_COLUMNS: [&str; 3] = ["id", "revision", "born_rev"];

const FINGERPRINT_PREFIX: &str = "fingerprint:";
const CACHE_TABLES: [&str; 6] = ["window_marker", "scope_member", "scope_revision", "scope_edge", "tombstone", "query_window"];

const CACHE_STATEMENTS: [&str; 18] = [
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

const CORE_STATEMENTS: [&str; 44] = [
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
  PRIMARY KEY (model, row_id, action)
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
  outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'http', 'unauthorized', 'revoked', 'session')),
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

#[derive(Debug, thiserror::Error)]
pub enum DdlError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Text,
    Integer,
    Real,
}

impl ColumnType {
    pub fn as_sql(self) -> &'static str {
        match self {
            ColumnType::Text => "TEXT",
            ColumnType::Integer => "INTEGER",
            ColumnType::Real => "REAL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum RecreateReason {
    UnreadableFile,
    SchemaVersion,
    CoreFormat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestVerdict {
    Fresh,
    Compatible,
    CacheOnly { models: Vec<String> },
    Incompatible,
}

pub fn column_name(field: &str) -> String {
    let mut name = String::with_capacity(field.len() + 4);
    for character in field.chars() {
        if character.is_uppercase() {
            if !name.is_empty() {
                name.push('_');
            }
            name.extend(character.to_lowercase());
        } else {
            name.push(character);
        }
    }
    name
}

pub fn table_name(model: &str) -> String {
    format!("row_{}", model.replace('-', "_"))
}

pub fn column_type(kind: &FieldKind) -> Option<ColumnType> {
    match kind {
        FieldKind::Scalar { kind: ScalarKind::Str | ScalarKind::Id | ScalarKind::Date } | FieldKind::Enum { .. } => {
            Some(ColumnType::Text)
        }
        FieldKind::Scalar { kind: ScalarKind::Int | ScalarKind::Bool } => Some(ColumnType::Integer),
        FieldKind::Scalar { kind: ScalarKind::Num } => Some(ColumnType::Real),
        FieldKind::Raw | FieldKind::Object | FieldKind::Array | FieldKind::List { .. } | FieldKind::Union | FieldKind::UnionArray | FieldKind::File => None,
    }
}

pub fn rev_column(column: &str) -> String {
    format!("rev__{column}")
}

pub(crate) fn sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn promoted_columns(model: &ModelSpec) -> impl Iterator<Item = (String, ColumnType)> + '_ {
    model
        .fields
        .iter()
        .filter(|field| field.indexed && field.name != "id")
        .filter_map(|field| column_type(&field.kind).map(|column| (column_name(&field.name), column)))
}

pub fn create_table_statement(model: &ModelSpec) -> String {
    let plan = layout(model);
    let mut definitions = vec![
        "id TEXT PRIMARY KEY NOT NULL".to_string(),
        "revision INTEGER NOT NULL".to_string(),
        "born_rev INTEGER NOT NULL".to_string(),
    ];
    definitions.extend(plan.row_columns().iter().map(|(name, ty)| format!("{} {}", sql_ident(name), ty.as_sql())));
    definitions.extend(plan.rev_columns().iter().map(|(_, column)| format!("{} INTEGER", sql_ident(column))));
    let kind = if model.is_memory() { "CREATE TEMP TABLE" } else { "CREATE TABLE" };
    format!(
        "{kind} IF NOT EXISTS {} (\n  {}\n) STRICT",
        table_name(model.key.as_str()),
        definitions.join(",\n  ")
    )
}

pub fn create_index_statements(model: &ModelSpec) -> Vec<String> {
    let table = table_name(model.key.as_str());
    let mut indexes: Vec<Vec<(String, bool)>> = promoted_columns(model).map(|(name, _)| vec![(sql_ident(&name), false)]).collect();
    indexes.extend(model.indexes.iter().map(|fields| fields.iter().map(|field| (sql_ident(&column_name(field)), false)).collect()));
    for relation in &model.relations {
        let mut fields: Vec<_> = relation.by_columns.iter().map(|name| (sql_ident(&column_name(name)), false)).collect();
        fields.sort();
        if let Sort::Fields(order) = &relation.sort {
            for field in order {
                let name = sql_ident(&column_name(&field.column));
                if !fields.iter().any(|(held, _)| held == &name) {
                    match field.direction {
                        crate::value::SortDirection::AscNullsLast => fields.push((format!("{name} IS NULL"), false)),
                        crate::value::SortDirection::DescNullsFirst => fields.push((format!("{name} IS NOT NULL"), false)),
                        _ => {}
                    }
                    fields.push((name, matches!(field.direction, crate::value::SortDirection::Desc | crate::value::SortDirection::DescNullsFirst)));
                }
            }
            if !order.is_empty() && !fields.iter().any(|(name, _)| name == "\"id\"") {
                fields.push((format!("({})", crate::identity::permanent_row_flag(&sql_ident("id"))), false));
                fields.push((sql_ident("id"), false));
            }
        }
        if !fields.is_empty() { indexes.push(fields); }
    }
    indexes.sort();
    indexes.dedup();
    indexes.iter().filter(|fields| !indexes.iter().any(|other| other.len() > fields.len() && other.starts_with(fields)))
        .enumerate().map(|(index, fields)| {
            let columns: Vec<_> = fields.iter().map(|(name, descending)| if *descending { format!("{name} DESC") } else { name.clone() }).collect();
            format!("CREATE INDEX IF NOT EXISTS {table}_idx_{index} ON {table} ({})", columns.join(", "))
        }).collect()
}

pub fn fts_name(model: &str) -> String {
    format!("fts_{}", table_name(model))
}

pub fn fts_statement(model: &ModelSpec) -> Option<String> {
    let count = model.search_paths().len();
    if count == 0 {
        return None;
    }
    let columns: Vec<String> = (0..count).map(|index| format!("c{index}")).collect();
    let schema = if model.is_memory() { "temp." } else { "" };
    Some(format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS {schema}{} USING fts5({}, tokenize = 'unicode61 remove_diacritics 2', prefix = '2 3 4')",
        fts_name(model.key.as_str()),
        columns.join(", ")
    ))
}

pub fn fts_tables(connection: &Connection) -> Result<Vec<String>, DdlError> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'fts\\_row\\_%' ESCAPE '\\' AND sql LIKE 'CREATE VIRTUAL%'
         UNION ALL SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name LIKE 'fts\\_row\\_%' ESCAPE '\\' AND sql LIKE 'CREATE VIRTUAL%'",
    )?;
    let names = statement.query_map([], |row| row.get(0))?;
    Ok(names.collect::<Result<_, _>>()?)
}

pub fn model_statements(model: &ModelSpec) -> Vec<String> {
    let mut statements = vec![create_table_statement(model)];
    statements.extend(fts_statement(model));
    statements.extend(layout(model).table_statements(model.is_memory()));
    statements.extend(create_index_statements(model));
    statements
}

pub fn models_statements(models: &[ModelSpec]) -> Vec<String> {
    models.iter().flat_map(model_statements).collect()
}

pub fn prepare_connection(connection: &Connection) -> Result<(), DdlError> {
    connection.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "foreign_keys", "OFF")?;
    connection.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

pub fn migrate_core(connection: &Connection) -> Result<(), DdlError> {
    let transaction = connection.unchecked_transaction()?;
    create_core_tables(&transaction)?;
    Ok(transaction.commit()?)
}

pub(crate) fn create_core_tables(connection: &Connection) -> Result<(), DdlError> {
    for statement in CORE_STATEMENTS.iter().chain(CACHE_STATEMENTS.iter()) {
        connection.execute_batch(statement)?;
    }
    connection.execute_batch(&format!("pragma user_version = {SCHEMA_VERSION}"))?;
    Ok(())
}

pub fn recreate_reason(connection: &Connection) -> Result<Option<RecreateReason>, DdlError> {
    let version: i64 = connection.query_row("pragma user_version", [], |row| row.get(0))?;
    if version != 0 && version != SCHEMA_VERSION {
        return Ok(Some(RecreateReason::SchemaVersion));
    }
    let has_manifest: bool = connection.query_row("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'manifest')", [], |row| row.get(0))?;
    if has_manifest && manifest_read(connection, FORMAT_VERSION_KEY)?.is_some_and(|stored| stored != format_version()) {
        return Ok(Some(RecreateReason::CoreFormat));
    }
    Ok(None)
}

const STORED_VALUE_FORMAT: &str = "dates are UTC text with milliseconds; rows are typed columns with rev__ columns; free values are node trees; relation parameters are type-tagged JSON values";

pub fn format_version() -> String {
    let text = [CORE_STATEMENTS.as_slice(), CACHE_STATEMENTS.as_slice(), &[STORED_VALUE_FORMAT]].concat().join("\n");
    Sha256::digest(text.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn rebuild_cache_tables(connection: &Connection) -> Result<(), DdlError> {
    for table in CACHE_TABLES {
        connection.execute_batch(&format!("DROP TABLE IF EXISTS {table}"))?;
    }
    for statement in CACHE_STATEMENTS {
        connection.execute_batch(statement)?;
    }
    Ok(())
}

pub fn create_model_tables(connection: &Connection, models: &[ModelSpec]) -> Result<(), DdlError> {
    let transaction = connection.unchecked_transaction()?;
    for statement in models_statements(models) {
        transaction.execute_batch(&statement)?;
    }
    Ok(transaction.commit()?)
}

#[derive(Serialize)]
struct RelationFingerprint<'a> {
    name: &'a str,
    by_columns: Vec<&'a str>,
    text_params: Vec<&'a str>,
    is_global: bool,
    sort: &'a Sort,
    membership: &'a BTreeMap<String, Membership>,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    row_rules: &'a [crate::schema::RowRule],
    joins_node: bool,
}

#[derive(Serialize)]
struct ModelFingerprint<'a> {
    fields: Vec<&'a FieldSpec>,
    relations: Vec<RelationFingerprint<'a>>,
    associations: Vec<&'a AssociationSpec>,
    storage: Storage,
    indexes: Vec<&'a Vec<String>>,
    ddl: Vec<String>,
}

pub fn fingerprint(model: &ModelSpec) -> String {
    Sha256::digest(fingerprint_text(model).as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn fingerprint_text(model: &ModelSpec) -> String {
    let mut fields: Vec<&FieldSpec> = model.fields.iter().collect();
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    let mut relations: Vec<RelationFingerprint> = model
        .relations
        .iter()
        .map(|relation| {
            let mut by_columns: Vec<&str> = relation.by_columns.iter().map(String::as_str).collect();
            by_columns.sort_unstable();
            let mut text_params: Vec<&str> = relation.text_params.iter().map(String::as_str).collect();
            text_params.sort_unstable();
            RelationFingerprint { name: &relation.name, by_columns, text_params, is_global: relation.is_global, sort: &relation.sort, membership: &relation.membership, row_rules: &relation.row_rules, joins_node: relation.joins_node }
        })
        .collect();
    relations.sort_by(|left, right| left.name.cmp(right.name));
    let mut associations: Vec<&AssociationSpec> = model.associations.iter().collect();
    associations.sort_by(|left, right| left.name.cmp(&right.name));
    let mut indexes: Vec<&Vec<String>> = model.indexes.iter().collect();
    indexes.sort();
    let mut canonical = model.clone();
    canonical.fields.sort_by(|left, right| left.name.cmp(&right.name));
    let mut ddl = vec![create_table_statement(&canonical)];
    ddl.extend(layout(&canonical).table_statements(canonical.is_memory()));
    ddl.extend(create_index_statements(&canonical));
    ddl.sort();
    serde_json::to_string(&ModelFingerprint { fields, relations, associations, storage: model.storage, indexes, ddl }).expect("fingerprint serializes")
}

pub fn fingerprint_key(model: &str) -> String {
    format!("{FINGERPRINT_PREFIX}{model}")
}

pub fn manifest_read(connection: &Connection, key: &str) -> Result<Option<String>, DdlError> {
    Ok(connection.query_row("SELECT value FROM manifest WHERE key = ?1", [key], |row| row.get(0)).optional()?)
}

pub fn manifest_write(connection: &Connection, key: &str, value: &str) -> Result<(), DdlError> {
    connection.execute(
        "INSERT INTO manifest (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

pub fn manifest_stamp(
    connection: &Connection,
    data_version: Option<&str>,
    fingerprints: &BTreeMap<String, String>,
) -> Result<(), DdlError> {
    manifest_write(connection, DATA_VERSION_KEY, data_version.unwrap_or(""))?;
    for (model, fingerprint) in fingerprints {
        manifest_write(connection, &fingerprint_key(model), fingerprint)?;
    }
    manifest_write(connection, FORMAT_VERSION_KEY, &format_version())?;
    Ok(())
}

fn cache_is_empty(connection: &Connection) -> Result<bool, DdlError> {
    for table in CACHE_TABLES {
        let used: bool = connection.query_row(&format!("SELECT EXISTS (SELECT 1 FROM {table})"), [], |row| row.get(0))?;
        if used {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn manifest_verify(
    connection: &Connection,
    data_version: Option<&str>,
    fingerprints: &BTreeMap<String, String>,
) -> Result<ManifestVerdict, DdlError> {
    let mut statement = connection.prepare("SELECT key, value FROM manifest")?;
    let values: BTreeMap<String, String> =
        statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;

    if values.is_empty() && cache_is_empty(connection)? {
        return Ok(ManifestVerdict::Fresh);
    }
    if values.get(FORMAT_VERSION_KEY).map(String::as_str) != Some(format_version().as_str()) {
        return Ok(ManifestVerdict::Incompatible);
    }
    let stored_data_version = values.get(DATA_VERSION_KEY).map_or("", String::as_str);
    if stored_data_version != data_version.unwrap_or("") {
        return Ok(ManifestVerdict::Incompatible);
    }

    let stored: BTreeMap<&str, &str> = values
        .iter()
        .filter_map(|(key, value)| key.strip_prefix(FINGERPRINT_PREFIX).map(|model| (model, value.as_str())))
        .collect();
    let mut mismatched: Vec<String> = stored
        .iter()
        .filter(|(model, fingerprint)| fingerprints.get(**model).is_none_or(|current| current != **fingerprint))
        .map(|(model, _)| model.to_string())
        .collect();
    mismatched.sort();
    if mismatched.is_empty() {
        return Ok(ManifestVerdict::Compatible);
    }
    Ok(ManifestVerdict::CacheOnly { models: mismatched })
}

#[cfg(test)]
mod ddl_tests;
