use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::schema::{FieldKind, FieldSpec, ModelSpec, Sort};

mod layout;
pub use layout::*;
use crate::value::ScalarKind;

pub const SCHEMA_VERSION: i64 = 1;

pub const FORMAT_VERSION_KEY: &str = "format_version";
pub const DATA_VERSION_KEY: &str = "data_version";
pub const REVISION_SEQ_KEY: &str = "revision_seq";

pub const SERVICE_COLUMNS: [&str; 3] = ["id", "revision", "born_rev"];

const FINGERPRINT_PREFIX: &str = "fingerprint:";
const CACHE_TABLES: [&str; 4] = ["scope_member", "tombstone", "query_window", "query_window_row"];

const CACHE_STATEMENTS: [&str; 8] = [
    "CREATE TABLE IF NOT EXISTS scope_member (
  model TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  row_id TEXT NOT NULL,
  order_key TEXT NOT NULL,
  PRIMARY KEY (model, scope_key, row_id)
) STRICT",
    "CREATE UNIQUE INDEX IF NOT EXISTS scope_walk ON scope_member (model, scope_key, order_key, row_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS scope_order_unique ON scope_member (model, scope_key, order_key)",
    "CREATE INDEX IF NOT EXISTS scope_member_row ON scope_member (model, row_id)",
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
  issued INTEGER NOT NULL,
  applied INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  count INTEGER NOT NULL,
  landed_at INTEGER NOT NULL,
  invalidated INTEGER NOT NULL DEFAULT 0 CHECK (invalidated IN (0, 1))
) STRICT",
    "CREATE TABLE IF NOT EXISTS query_window_row (
  key TEXT NOT NULL,
  pos INTEGER NOT NULL,
  row_id TEXT NOT NULL,
  PRIMARY KEY (key, pos)
) STRICT, WITHOUT ROWID",
];

const CORE_STATEMENTS: [&str; 23] = [
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
  attempts INTEGER NOT NULL DEFAULT 0,
  since INTEGER NOT NULL,
  error_kind TEXT,
  error_message TEXT,
  up_phase TEXT,
  up_stage TEXT,
  up_scenario TEXT,
  up_total INTEGER,
  up_sent INTEGER,
  up_retries INTEGER,
  up_key TEXT,
  up_upload_id TEXT,
  up_signed_id TEXT,
  up_blob_id TEXT
) STRICT",
    "CREATE TABLE IF NOT EXISTS op_row (
  seq INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  row_id TEXT NOT NULL,
  PRIMARY KEY (seq, pos)
) STRICT, WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS op_row_id ON op_row (row_id)",
    "CREATE TABLE IF NOT EXISTS op_part (
  seq INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  number INTEGER NOT NULL,
  etag TEXT NOT NULL,
  PRIMARY KEY (seq, pos)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS op_value (
  seq INTEGER NOT NULL,
  part TEXT NOT NULL,
  node INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  name TEXT,
  kind INTEGER NOT NULL,
  whole INTEGER,
  fraction REAL,
  text TEXT,
  PRIMARY KEY (seq, part, node)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS row_sync (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'sending', 'failed', 'delivery_unknown')),
  error_kind TEXT,
  error_message TEXT,
  attempts INTEGER NOT NULL,
  since INTEGER NOT NULL,
  up_status TEXT,
  up_stage TEXT,
  up_sent INTEGER,
  up_total INTEGER,
  up_wait TEXT,
  up_attempts INTEGER,
  up_error_kind TEXT,
  up_error_message TEXT,
  PRIMARY KEY (model, row_id)
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
    "CREATE TABLE IF NOT EXISTS op_once (
  namespace TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  PRIMARY KEY (namespace, dedupe_key)
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
  body TEXT NOT NULL
) STRICT",
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
        FieldKind::Raw | FieldKind::Object | FieldKind::Array | FieldKind::List { .. } | FieldKind::File => None,
    }
}

pub fn rev_column(column: &str) -> String {
    format!("rev__{column}")
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
    definitions.extend(plan.row_columns().into_iter().map(|(name, ty)| format!("{name} {}", ty.as_sql())));
    definitions.extend(plan.rev_columns().into_iter().map(|(_, column)| format!("{column} INTEGER")));
    let kind = if model.is_memory() { "CREATE TEMP TABLE" } else { "CREATE TABLE" };
    format!(
        "{kind} IF NOT EXISTS {} (\n  {}\n) STRICT",
        table_name(model.key.as_str()),
        definitions.join(",\n  ")
    )
}

pub fn create_index_statements(model: &ModelSpec) -> Vec<String> {
    let table = table_name(model.key.as_str());
    let mut indexes: Vec<Vec<(String, bool)>> = promoted_columns(model).map(|(name, _)| vec![(name, false)]).collect();
    indexes.extend(model.indexes.iter().map(|fields| fields.iter().map(|field| (column_name(field), false)).collect()));
    for relation in &model.relations {
        let mut fields: Vec<_> = relation.by_columns.iter().map(|name| (column_name(name), false)).collect();
        fields.sort();
        if let Sort::Fields(order) = &relation.sort {
            for field in order {
                let name = column_name(&field.column);
                if !fields.iter().any(|(held, _)| held == &name) {
                    match field.direction {
                        crate::value::SortDirection::AscNullsLast => fields.push((format!("{name} IS NULL"), false)),
                        crate::value::SortDirection::DescNullsFirst => fields.push((format!("{name} IS NOT NULL"), false)),
                        _ => {}
                    }
                    fields.push((name, matches!(field.direction, crate::value::SortDirection::Desc | crate::value::SortDirection::DescNullsFirst)));
                }
            }
            if !order.is_empty() && !fields.iter().any(|(name, _)| name == "id") { fields.push(("id".into(), false)); }
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
    for statement in CORE_STATEMENTS.iter().chain(CACHE_STATEMENTS.iter()) {
        transaction.execute_batch(statement)?;
    }
    transaction.execute_batch(&format!("pragma user_version = {SCHEMA_VERSION}"))?;
    Ok(transaction.commit()?)
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

const STORED_VALUE_FORMAT: &str = "dates are UTC text with milliseconds; rows are typed columns with rev__ columns; free values are node trees";

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
}

#[derive(Serialize)]
struct ModelFingerprint<'a> {
    fields: Vec<&'a FieldSpec>,
    relations: Vec<RelationFingerprint<'a>>,
    indexes: Vec<&'a Vec<String>>,
    ddl: Vec<String>,
}

pub fn fingerprint(model: &ModelSpec) -> String {
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
            RelationFingerprint { name: &relation.name, by_columns, text_params, is_global: relation.is_global, sort: &relation.sort }
        })
        .collect();
    relations.sort_by(|left, right| left.name.cmp(right.name));
    let mut indexes: Vec<&Vec<String>> = model.indexes.iter().collect();
    indexes.sort();
    let mut canonical = model.clone();
    canonical.fields.sort_by(|left, right| left.name.cmp(&right.name));
    let mut ddl = vec![create_table_statement(&canonical)];
    ddl.extend(layout(&canonical).table_statements(canonical.is_memory()));
    ddl.extend(create_index_statements(&canonical));
    ddl.sort();
    let text = serde_json::to_string(&ModelFingerprint { fields, relations, indexes, ddl }).expect("fingerprint serializes");
    Sha256::digest(text.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
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
    manifest_write(connection, FORMAT_VERSION_KEY, &format_version())?;
    manifest_write(connection, DATA_VERSION_KEY, data_version.unwrap_or(""))?;
    for (model, fingerprint) in fingerprints {
        manifest_write(connection, &fingerprint_key(model), fingerprint)?;
    }
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
