use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::schema::{FieldKind, FieldSpec, ModelSpec, Sort};
use crate::value::ScalarKind;

pub const RECORD_VERSION: i64 = 1;
pub const SCHEMA_VERSION: i64 = 1;

pub const FORMAT_VERSION_KEY: &str = "format_version";
pub const RECORD_VERSION_KEY: &str = "record_version";
pub const DATA_VERSION_KEY: &str = "data_version";
pub const REVISION_SEQ_KEY: &str = "revision_seq";
pub const RESET_INTENT_KEY: &str = "reset_intent";

pub const SERVICE_COLUMNS: [&str; 6] = ["id", "revision", "born_rev", "field_rev", "record_version", "payload"];

const FINGERPRINT_PREFIX: &str = "fingerprint:";
const CACHE_TABLES: [&str; 3] = ["scope_member", "tombstone", "query_window"];

const CACHE_STATEMENTS: [&str; 7] = [
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
  ids BLOB NOT NULL,
  issued INTEGER NOT NULL,
  applied INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  count INTEGER NOT NULL,
  landed_at INTEGER NOT NULL,
  invalidated INTEGER NOT NULL DEFAULT 0 CHECK (invalidated IN (0, 1))
) STRICT",
];

const CORE_STATEMENTS: [&str; 12] = [
    "CREATE TABLE IF NOT EXISTS manifest (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT NOT NULL
) STRICT",
    "CREATE TABLE IF NOT EXISTS op (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  action_key TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'failed', 'delivery_unknown', 'committed', 'rolledback')),
  row_ids BLOB NOT NULL,
  temp_id TEXT,
  input BLOB NOT NULL,
  transport_started INTEGER NOT NULL DEFAULT 0,
  base_revision INTEGER,
  created_at INTEGER NOT NULL,
  dedupe_key TEXT,
  rollback BLOB,
  attempts INTEGER NOT NULL DEFAULT 0,
  error BLOB,
  since INTEGER NOT NULL,
  upload BLOB
) STRICT",
    "CREATE TABLE IF NOT EXISTS row_sync (
  model TEXT NOT NULL,
  row_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'sending', 'failed', 'delivery_unknown')),
  actions BLOB NOT NULL,
  dirty_fields BLOB NOT NULL,
  error BLOB,
  attempts INTEGER NOT NULL,
  since INTEGER NOT NULL,
  upload BLOB,
  PRIMARY KEY (model, row_id)
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
  payload BLOB NOT NULL,
  reason TEXT NOT NULL
) STRICT",
    "CREATE INDEX IF NOT EXISTS quarantine_reason ON quarantine (reason, seq)",
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
  value BLOB NOT NULL,
  PRIMARY KEY (model, row_id, path)
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

fn promoted_columns(model: &ModelSpec) -> impl Iterator<Item = (String, ColumnType)> + '_ {
    model
        .fields
        .iter()
        .filter(|field| field.indexed)
        .filter_map(|field| column_type(&field.kind).map(|column| (column_name(&field.name), column)))
}

pub fn create_table_statement(model: &ModelSpec) -> String {
    let mut definitions = vec![
        "id TEXT PRIMARY KEY NOT NULL".to_string(),
        "revision INTEGER NOT NULL".to_string(),
        "born_rev INTEGER NOT NULL".to_string(),
        "field_rev BLOB NOT NULL".to_string(),
        "record_version INTEGER NOT NULL".to_string(),
        "payload BLOB NOT NULL".to_string(),
    ];
    definitions.extend(promoted_columns(model).map(|(name, column)| format!("{name} {}", column.as_sql())));
    let kind = if model.is_memory() { "CREATE TEMP TABLE" } else { "CREATE TABLE" };
    format!(
        "{kind} IF NOT EXISTS {} (\n  {}\n) STRICT",
        table_name(model.key.as_str()),
        definitions.join(",\n  ")
    )
}

pub fn create_index_statements(model: &ModelSpec) -> Vec<String> {
    let table = table_name(model.key.as_str());
    let singles = promoted_columns(model)
        .map(|(name, _)| format!("CREATE INDEX IF NOT EXISTS {table}_{name} ON {table} ({name})"));
    let composites = model.composite_indexes.iter().map(|fields| {
        let names: Vec<String> = fields.iter().map(|field| column_name(field)).collect();
        format!("CREATE INDEX IF NOT EXISTS {table}_{} ON {table} ({})", names.join("_"), names.join(", "))
    });
    singles.chain(composites).collect()
}

pub fn model_statements(model: &ModelSpec) -> Vec<String> {
    let mut statements = vec![create_table_statement(model)];
    statements.extend(create_index_statements(model));
    statements
}

pub fn models_statements(models: &[ModelSpec]) -> Vec<String> {
    models.iter().flat_map(model_statements).collect()
}

pub fn stale_version_delete(model: &str) -> String {
    format!("DELETE FROM {} WHERE record_version <> {RECORD_VERSION}", table_name(model))
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

const STORED_VALUE_FORMAT: &str = "dates are UTC text with milliseconds";

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
    composite_indexes: Vec<&'a Vec<String>>,
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
    let mut composite_indexes: Vec<&Vec<String>> = model.composite_indexes.iter().collect();
    composite_indexes.sort();
    serde_json::to_string(&ModelFingerprint { fields, relations, composite_indexes }).expect("fingerprint serializes")
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
    manifest_write(connection, RECORD_VERSION_KEY, &RECORD_VERSION.to_string())?;
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
