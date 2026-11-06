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
pub(crate) const CARRIED_TABLES: [&str; 24] = ["op", "op_transfer", "transfer_outcome", "op_item", "op_item_retry", "op_item_header", "op_row", "op_part", "op_value", "op_error", "transfer_abort", "blob_upload", "blob_use", "encrypted_session", "encrypted_pending", "encrypted_answer", "sync_upload", "sync_upload_part", "sync_upload_error", "local_pending", "file", "restore_failed", "upload_orphan", "sync_upload_batch"];

pub const SERVICE_COLUMNS: [&str; 3] = ["id", "revision", "born_rev"];

const FINGERPRINT_PREFIX: &str = "fingerprint:";
const CACHE_TABLES: [&str; 6] = ["window_marker", "scope_member", "scope_revision", "scope_edge", "tombstone", "query_window"];

mod statements;
use statements::{CACHE_STATEMENTS, CORE_STATEMENTS};

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
    Match,
    CacheOnly { models: Vec<String> },
    Mismatch,
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
        return Ok(ManifestVerdict::Mismatch);
    }
    let stored_data_version = values.get(DATA_VERSION_KEY).map_or("", String::as_str);
    if stored_data_version != data_version.unwrap_or("") {
        return Ok(ManifestVerdict::Mismatch);
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
        return Ok(ManifestVerdict::Match);
    }
    Ok(ManifestVerdict::CacheOnly { models: mismatched })
}

#[cfg(test)]
mod ddl_tests;
