use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::schema::{FieldKind, FieldSpec, ModelSpec, Sort};
use crate::value::ScalarKind;

pub const FORMAT_VERSION: i64 = 1;
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

const CORE_STATEMENTS: [&str; 15] = [
    "CREATE TABLE manifest (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT NOT NULL
) STRICT",
    "CREATE TABLE scope_member (
  model TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  row_id TEXT NOT NULL,
  order_key TEXT NOT NULL,
  PRIMARY KEY (model, scope_key, row_id)
) STRICT",
    "CREATE UNIQUE INDEX scope_walk ON scope_member (model, scope_key, order_key, row_id)",
    "CREATE UNIQUE INDEX scope_order_unique ON scope_member (model, scope_key, order_key)",
    "CREATE INDEX scope_member_row ON scope_member (model, row_id)",
    "CREATE TABLE tombstone (
  model TEXT NOT NULL,
  id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  deleted_at INTEGER NOT NULL,
  PRIMARY KEY (model, id)
) STRICT, WITHOUT ROWID",
    "CREATE INDEX tombstone_age ON tombstone (deleted_at)",
    "CREATE TABLE query_window (
  key TEXT PRIMARY KEY NOT NULL,
  cursor TEXT,
  has_next INTEGER NOT NULL,
  ids BLOB NOT NULL,
  issued INTEGER NOT NULL,
  applied INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  count INTEGER NOT NULL,
  landed_at INTEGER NOT NULL,
  invalidated INTEGER NOT NULL DEFAULT 0 CHECK (invalidated IN (0, 1))
) STRICT",
    "CREATE TABLE op (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  action_key TEXT NOT NULL,
  action_mode TEXT NOT NULL CHECK (action_mode IN ('request', 'durable')),
  status TEXT NOT NULL CHECK (status IN ('pending', 'failed', 'delivery_unknown', 'committed', 'rolledback')),
  row_ids BLOB NOT NULL,
  temp_id TEXT,
  input BLOB NOT NULL,
  transport_started INTEGER NOT NULL DEFAULT 0,
  base_revision INTEGER,
  created_at INTEGER NOT NULL,
  dedupe_key TEXT,
  rollback BLOB
) STRICT",
    "CREATE INDEX op_open ON op (action_key, status)",
    "CREATE INDEX op_temp ON op (temp_id)",
    "CREATE UNIQUE INDEX op_open_dedupe ON op (action_key, dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'delivery_unknown')",
    "CREATE TABLE op_once (
  namespace TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  PRIMARY KEY (namespace, dedupe_key)
) STRICT, WITHOUT ROWID",
    "CREATE TABLE quarantine (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  model TEXT,
  payload BLOB NOT NULL,
  reason TEXT NOT NULL
) STRICT",
    "CREATE INDEX quarantine_reason ON quarantine (reason, seq)",
];

#[derive(Debug, thiserror::Error)]
pub enum DdlError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("database schema version {found} is newer than this build")]
    SchemaVersionAhead { found: i64 },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaVerdict {
    Fresh,
    Compatible,
    Stale,
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
        FieldKind::Raw | FieldKind::Object | FieldKind::Array | FieldKind::List { .. } => None,
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
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n) STRICT",
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

pub fn table_names(models: &[ModelSpec]) -> Vec<String> {
    models.iter().map(|model| table_name(model.key.as_str())).collect()
}

pub fn drop_table_statement(model: &str) -> String {
    format!("DROP TABLE IF EXISTS {}", table_name(model))
}

pub fn stale_version_delete(model: &str) -> String {
    format!("DELETE FROM {} WHERE record_version <> {RECORD_VERSION}", table_name(model))
}

pub fn expired_temp_rows_delete(model: &str, protected_count: usize) -> String {
    let protected = if protected_count == 0 {
        String::new()
    } else {
        format!(" AND id NOT IN ({})", vec!["?"; protected_count].join(", "))
    };
    format!("DELETE FROM {} WHERE id LIKE 'temp-%' AND created_at < ?{protected}", table_name(model))
}

pub fn prepare_connection(connection: &Connection) -> Result<(), DdlError> {
    connection.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "foreign_keys", "OFF")?;
    Ok(())
}

pub fn migrate_core(connection: &Connection) -> Result<(), DdlError> {
    let version: i64 = connection.query_row("pragma user_version", [], |row| row.get(0))?;
    match version {
        SCHEMA_VERSION => Ok(()),
        0 => {
            let transaction = connection.unchecked_transaction()?;
            for statement in CORE_STATEMENTS {
                transaction.execute_batch(statement)?;
            }
            transaction.execute_batch(&format!("pragma user_version = {SCHEMA_VERSION}"))?;
            Ok(transaction.commit()?)
        }
        found => Err(DdlError::SchemaVersionAhead { found }),
    }
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
            RelationFingerprint { name: &relation.name, by_columns, is_global: relation.is_global, sort: &relation.sort }
        })
        .collect();
    relations.sort_by(|left, right| left.name.cmp(right.name));
    let mut composite_indexes: Vec<&Vec<String>> = model.composite_indexes.iter().collect();
    composite_indexes.sort();
    serde_json::to_string(&ModelFingerprint { fields, relations, composite_indexes }).expect("fingerprint serializes")
}

pub fn schema_verdicts(
    stored: &BTreeMap<String, String>,
    current: &BTreeMap<String, String>,
) -> BTreeMap<String, SchemaVerdict> {
    let models = stored.keys().chain(current.keys());
    models
        .map(|model| {
            let verdict = match (stored.get(model), current.get(model)) {
                (None, _) => SchemaVerdict::Fresh,
                (Some(_), None) => SchemaVerdict::Stale,
                (Some(before), Some(after)) if before == after => SchemaVerdict::Compatible,
                (Some(_), Some(_)) => SchemaVerdict::Stale,
            };
            (model.clone(), verdict)
        })
        .collect()
}

pub fn models_needing_reset(verdicts: &BTreeMap<String, SchemaVerdict>) -> Vec<String> {
    verdicts.iter().filter(|(_, verdict)| **verdict == SchemaVerdict::Stale).map(|(model, _)| model.clone()).collect()
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
    manifest_write(connection, FORMAT_VERSION_KEY, &FORMAT_VERSION.to_string())?;
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
    if values.get(FORMAT_VERSION_KEY).map(String::as_str) != Some(FORMAT_VERSION.to_string().as_str()) {
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
mod tests {
    use super::*;
    use crate::schema::{RelationSpec, SortField};
    use crate::value::{ScalarKind, SortDirection};
    use rusqlite::Connection;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    fn messages() -> ModelSpec {
        ModelSpec::new(
            "messages",
            vec![
                FieldSpec::scalar("id", ScalarKind::Id),
                FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
                FieldSpec::scalar("sequenceNumber", ScalarKind::Int).nullable().indexed(),
                FieldSpec::scalar("score", ScalarKind::Num).nullable().indexed(),
                FieldSpec::scalar("createdAt", ScalarKind::Date).indexed(),
                FieldSpec::object("media").nullable(),
            ],
        )
    }

    fn groups() -> ModelSpec {
        ModelSpec::new("groups", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str).indexed()])
    }

    fn core_connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        migrate_core(&connection).unwrap();
        connection
    }

    fn names(connection: &Connection, kind: &str) -> BTreeSet<String> {
        let mut statement = connection.prepare("select name from sqlite_master where type = ?1").unwrap();
        statement.query_map([kind], |row| row.get::<_, String>(0)).unwrap().map(Result::unwrap).collect()
    }

    fn columns(connection: &Connection, table: &str) -> BTreeMap<String, (String, bool)> {
        let mut statement = connection.prepare(&format!("pragma table_info({table})")).unwrap();
        statement
            .query_map([], |row| Ok((row.get::<_, String>(1)?, (row.get::<_, String>(2)?, row.get::<_, i64>(3)? == 1))))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn sqlite_supports_strict_tables() {
        let connection = Connection::open_in_memory().unwrap();
        let version: String = connection.query_row("select sqlite_version()", [], |row| row.get(0)).unwrap();
        let parts: Vec<u32> = version.split('.').map(|part| part.parse().unwrap()).collect();
        assert!((parts[0], parts[1]) >= (3, 37), "sqlite {version}");
    }

    #[test]
    fn column_and_table_names_follow_the_spec() {
        assert_eq!(column_name("chatId"), "chat_id");
        assert_eq!(column_name("sequenceNumber"), "sequence_number");
        assert_eq!(column_name("id"), "id");
        assert_eq!(table_name("messages"), "row_messages");
        assert_eq!(table_name("chat-room"), "row_chat_room");
    }

    #[test]
    fn column_types_follow_field_kinds() {
        let kind = |field: FieldSpec| column_type(&field.kind);
        assert_eq!(kind(FieldSpec::scalar("a", ScalarKind::Str)), Some(ColumnType::Text));
        assert_eq!(kind(FieldSpec::scalar("a", ScalarKind::Id)), Some(ColumnType::Text));
        assert_eq!(kind(FieldSpec::scalar("a", ScalarKind::Date)), Some(ColumnType::Text));
        assert_eq!(kind(FieldSpec::enumeration("a", &["x"])), Some(ColumnType::Text));
        assert_eq!(kind(FieldSpec::scalar("a", ScalarKind::Int)), Some(ColumnType::Integer));
        assert_eq!(kind(FieldSpec::scalar("a", ScalarKind::Bool)), Some(ColumnType::Integer));
        assert_eq!(kind(FieldSpec::scalar("a", ScalarKind::Num)), Some(ColumnType::Real));
        assert_eq!(kind(FieldSpec::object("a")), None);
        assert_eq!(kind(FieldSpec::array("a")), None);
        assert_eq!(kind(FieldSpec::raw("a")), None);
        assert_eq!(kind(FieldSpec::list("a", ScalarKind::Int)), None);
    }

    #[test]
    fn create_table_includes_service_and_indexed_columns() {
        let sql = create_table_statement(&messages());

        assert!(sql.contains("row_messages"));
        assert!(sql.contains("id TEXT PRIMARY KEY NOT NULL"));
        assert!(sql.contains("revision INTEGER NOT NULL"));
        assert!(sql.contains("born_rev INTEGER NOT NULL"));
        assert!(sql.contains("field_rev BLOB NOT NULL"));
        assert!(sql.contains("record_version INTEGER NOT NULL"));
        assert!(sql.contains("payload BLOB NOT NULL"));
        assert!(sql.contains("chat_id TEXT"));
        assert!(sql.contains("sequence_number INTEGER"));
        assert!(sql.contains("score REAL"));
        assert!(sql.contains("created_at TEXT"));
        assert!(!sql.contains("media"));
        assert!(sql.ends_with(") STRICT"));
    }

    #[test]
    fn create_indexes_include_columns_and_composites() {
        let model = ModelSpec { composite_indexes: vec![vec!["chatId".into(), "sequenceNumber".into()]], ..messages() };
        let statements = create_index_statements(&model);

        assert_eq!(statements.len(), 5);
        assert_eq!(statements[0], "CREATE INDEX IF NOT EXISTS row_messages_chat_id ON row_messages (chat_id)");
        assert_eq!(
            statements.last().unwrap(),
            "CREATE INDEX IF NOT EXISTS row_messages_chat_id_sequence_number ON row_messages (chat_id, sequence_number)"
        );
    }

    #[test]
    fn maintenance_deletes_use_current_version_and_protected_placeholders() {
        let with_two = expired_temp_rows_delete("messages", 2);
        let bare = "DELETE FROM row_messages WHERE id LIKE 'temp-%' AND created_at < ?";

        assert_eq!(stale_version_delete("messages"), "DELETE FROM row_messages WHERE record_version <> 1");
        assert!(with_two.starts_with("DELETE FROM row_messages"));
        assert!(with_two.contains("id NOT IN (?, ?)"));
        assert_eq!(expired_temp_rows_delete("messages", 0), bare);
        assert_eq!(drop_table_statement("messages"), "DROP TABLE IF EXISTS row_messages");
    }

    #[test]
    fn model_statements_keep_model_order() {
        let specs = [groups(), messages()];
        let statements = models_statements(&specs);
        let groups_at = statements.iter().position(|sql| sql.contains("CREATE TABLE IF NOT EXISTS row_groups")).unwrap();
        let messages_at = statements.iter().position(|sql| sql.contains("CREATE TABLE IF NOT EXISTS row_messages")).unwrap();

        assert!(groups_at < messages_at);
        assert_eq!(table_names(&specs), vec!["row_groups".to_string(), "row_messages".to_string()]);
        assert_eq!(model_statements(&groups()).len(), 2);
    }

    #[test]
    fn model_tables_apply_once_and_again() {
        let connection = Connection::open_in_memory().unwrap();
        let specs = [groups(), messages()];

        create_model_tables(&connection, &specs).unwrap();
        create_model_tables(&connection, &specs).unwrap();

        let tables = names(&connection, "table");
        assert!(tables.contains("row_groups") && tables.contains("row_messages"));
        let indexes = names(&connection, "index");
        assert!(indexes.contains("row_messages_chat_id") && indexes.contains("row_groups_title"));
    }

    #[test]
    fn row_table_matches_spec_columns() {
        let connection = Connection::open_in_memory().unwrap();
        create_model_tables(&connection, &[messages()]).unwrap();
        let found = columns(&connection, "row_messages");

        assert_eq!(found["id"], ("TEXT".to_string(), true));
        assert_eq!(found["revision"], ("INTEGER".to_string(), true));
        assert_eq!(found["born_rev"], ("INTEGER".to_string(), true));
        assert_eq!(found["field_rev"], ("BLOB".to_string(), true));
        assert_eq!(found["record_version"], ("INTEGER".to_string(), true));
        assert_eq!(found["payload"], ("BLOB".to_string(), true));
        assert_eq!(found["chat_id"], ("TEXT".to_string(), false));
        assert_eq!(found["sequence_number"], ("INTEGER".to_string(), false));
        assert_eq!(found["score"], ("REAL".to_string(), false));
        assert_eq!(found.len(), 10);
    }

    #[test]
    fn integer_lands_in_an_indexed_column() {
        let connection = Connection::open_in_memory().unwrap();
        create_model_tables(&connection, &[messages()]).unwrap();

        connection
            .execute(
                "insert into row_messages (id, revision, born_rev, field_rev, record_version, payload, sequence_number) values (?1, 1, 1, x'7b7d', 1, x'7b7d', ?2)",
                rusqlite::params!["row-1", 7],
            )
            .unwrap();
        let (value, storage): (i64, String) = connection
            .query_row("select sequence_number, typeof(sequence_number) from row_messages where id = 'row-1'", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();

        assert_eq!((value, storage.as_str()), (7, "integer"));
    }

    #[test]
    fn strict_row_table_rejects_wrong_column_type() {
        let connection = Connection::open_in_memory().unwrap();
        create_model_tables(&connection, &[messages()]).unwrap();

        let result = connection.execute(
            "insert into row_messages (id, revision, born_rev, field_rev, record_version, payload, sequence_number) values ('r', 1, 1, x'00', 1, x'00', 'abc')",
            [],
        );

        assert!(result.is_err());
    }

    #[test]
    fn core_migration_creates_tables_and_indexes() {
        let connection = core_connection();
        let tables: BTreeSet<String> =
            ["manifest", "scope_member", "tombstone", "query_window", "op", "op_once", "quarantine"].map(String::from).into();
        let indexes: BTreeSet<String> = [
            "scope_walk",
            "scope_order_unique",
            "scope_member_row",
            "tombstone_age",
            "op_open",
            "op_temp",
            "op_open_dedupe",
            "quarantine_reason",
        ]
        .map(String::from)
        .into();

        assert!(names(&connection, "table").is_superset(&tables));
        assert!(names(&connection, "index").is_superset(&indexes));
    }

    #[test]
    fn core_constraints_reject_duplicate_scope_order_and_unknown_op_status() {
        let connection = core_connection();
        let scope = "insert into scope_member (model, scope_key, row_id, order_key) values ('Chat', 'list', ?1, 'A')";
        connection.execute(scope, ["1"]).unwrap();
        let duplicate = connection.execute(scope, ["2"]);
        let op = |status: &str| {
            connection.execute(
                "insert into op (action_key, action_mode, status, row_ids, input, created_at) values ('Chat:send', 'request', ?1, x'00', x'00', 0)",
                [status],
            )
        };

        assert!(duplicate.is_err());
        assert!(op("unknown").is_err());
        assert!(op("pending").is_ok());
    }

    #[test]
    fn open_dedupe_key_is_unique_only_while_open() {
        let connection = core_connection();
        let insert = |status: &str, dedupe: Option<&str>| {
            connection.execute(
                "insert into op (action_key, action_mode, status, row_ids, input, created_at, dedupe_key) values ('Chat:send', 'durable', ?1, x'00', x'00', 0, ?2)",
                rusqlite::params![status, dedupe],
            )
        };

        assert!(insert("pending", Some("k")).is_ok());
        assert!(insert("pending", Some("k")).is_err());
        assert!(insert("delivery_unknown", Some("k")).is_err());
        assert!(insert("failed", Some("k")).is_ok());
        assert!(insert("pending", None).is_ok());
        assert!(insert("pending", None).is_ok());
    }

    #[test]
    fn core_table_columns_match_spec() {
        let connection = core_connection();
        let op = columns(&connection, "op");
        let window = columns(&connection, "query_window");

        let op_names: BTreeSet<&str> = op.keys().map(String::as_str).collect();
        let expected_op = [
            "seq",
            "action_key",
            "action_mode",
            "status",
            "row_ids",
            "temp_id",
            "input",
            "transport_started",
            "base_revision",
            "created_at",
            "dedupe_key",
            "rollback",
        ];
        assert_eq!(op_names, expected_op.into_iter().collect());
        assert!(!op["dedupe_key"].1 && !op["rollback"].1 && !op["temp_id"].1 && !op["base_revision"].1);
        assert!(op["input"].1 && op["row_ids"].1);
        assert_eq!(window.len(), 10);
        assert!(window["landed_at"].1 && !window["cursor"].1);
        let bad_flag = connection.execute(
            "insert into query_window (key, has_next, ids, issued, applied, generation, count, landed_at, invalidated) values ('k', 0, x'00', 0, 0, 0, 0, 0, 2)",
            [],
        );
        assert!(bad_flag.is_err());
    }

    #[test]
    fn core_migration_is_idempotent_and_stamps_schema_version() {
        let connection = core_connection();
        let before = names(&connection, "table");

        migrate_core(&connection).unwrap();

        assert_eq!(names(&connection, "table"), before);
        let version: i64 = connection.query_row("pragma user_version", [], |row| row.get(0)).unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn core_migration_rejects_a_newer_schema_version() {
        let connection = Connection::open_in_memory().unwrap();
        connection.pragma_update(None, "user_version", SCHEMA_VERSION + 1).unwrap();

        assert!(matches!(migrate_core(&connection), Err(DdlError::SchemaVersionAhead { found }) if found == SCHEMA_VERSION + 1));
    }

    #[test]
    fn prepared_connection_uses_normal_sync_and_no_foreign_keys() {
        let connection = Connection::open_in_memory().unwrap();
        prepare_connection(&connection).unwrap();
        let synchronous: i64 = connection.query_row("pragma synchronous", [], |row| row.get(0)).unwrap();
        let foreign_keys: i64 = connection.query_row("pragma foreign_keys", [], |row| row.get(0)).unwrap();

        assert_eq!((synchronous, foreign_keys), (1, 0));
    }

    #[test]
    fn fingerprint_ignores_declaration_order() {
        let fields = vec![
            FieldSpec::scalar("title", ScalarKind::Str).nullable(),
            FieldSpec::scalar("count", ScalarKind::Int).indexed().with_default(json!(0)),
            FieldSpec::scalar("createdAt", ScalarKind::Date),
        ];
        let relations = vec![RelationSpec::new("author", &["tenantId", "authorId"]), RelationSpec::new("comments", &["postId", "position"])];
        let first = ModelSpec { relations: relations.clone(), ..ModelSpec::new("posts", fields.clone()) };
        let second = ModelSpec {
            relations: vec![RelationSpec::new("comments", &["position", "postId"]), RelationSpec::new("author", &["authorId", "tenantId"])],
            ..ModelSpec::new("posts", vec![fields[2].clone(), fields[0].clone(), fields[1].clone()])
        };

        assert_eq!(fingerprint(&first), fingerprint(&second));
    }

    #[test]
    fn field_and_relation_changes_change_the_fingerprint() {
        let field = |field: FieldSpec| fingerprint(&ModelSpec::new("posts", vec![field]));
        let relation = |relation: RelationSpec| fingerprint(&ModelSpec { relations: vec![relation], ..ModelSpec::new("posts", vec![FieldSpec::raw("id")]) });
        let title = || FieldSpec::scalar("title", ScalarKind::Str).indexed();
        let original = field(title());
        let relation_original = relation(RelationSpec::new("author", &["authorId"]));

        assert_ne!(original, field(FieldSpec::scalar("title", ScalarKind::Int).indexed()));
        assert_ne!(original, field(title().nullable()));
        assert_ne!(original, field(title().with_default(json!("draft"))));
        assert_ne!(original, field(FieldSpec::scalar("title", ScalarKind::Str)));
        assert_ne!(original, field(title().optional()));
        assert_ne!(relation_original, relation(RelationSpec::new("author", &["authorId", "tenantId"])));
        assert_ne!(relation_original, relation(RelationSpec::new("owner", &["authorId"])));
        assert_ne!(
            relation_original,
            relation(RelationSpec {
                sort: Sort::Fields(vec![SortField { column: "authorId".into(), direction: SortDirection::Desc }]),
                ..RelationSpec::new("author", &["authorId"])
            })
        );
    }

    #[test]
    fn verdicts_cover_all_models_and_stale_ones_are_sorted() {
        let stored = BTreeMap::from([("changed", "old"), ("same", "same"), ("removed", "removed")].map(|(k, v)| (k.to_string(), v.to_string())));
        let current = BTreeMap::from([("new", "new"), ("changed", "new"), ("same", "same")].map(|(k, v)| (k.to_string(), v.to_string())));
        let verdicts: BTreeMap<String, SchemaVerdict> = [
            ("zeta", SchemaVerdict::Stale),
            ("alpha", SchemaVerdict::Stale),
            ("middle", SchemaVerdict::Compatible),
            ("beta", SchemaVerdict::Stale),
        ]
        .map(|(k, v)| (k.to_string(), v))
        .into();

        assert_eq!(
            schema_verdicts(&stored, &current),
            [
                ("new", SchemaVerdict::Fresh),
                ("same", SchemaVerdict::Compatible),
                ("changed", SchemaVerdict::Stale),
                ("removed", SchemaVerdict::Stale)
            ]
            .map(|(k, v)| (k.to_string(), v))
            .into()
        );
        assert_eq!(models_needing_reset(&verdicts), vec!["alpha", "beta", "zeta"]);
    }

    fn verdict(writes: &[(&str, &str)], data_version: Option<&str>, fingerprints: &[(&str, &str)]) -> ManifestVerdict {
        let connection = core_connection();
        for (key, value) in writes {
            manifest_write(&connection, key, value).unwrap();
        }
        let fingerprints = fingerprints.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        manifest_verify(&connection, data_version, &fingerprints).unwrap()
    }

    #[test]
    fn manifest_write_and_read_round_trip() {
        let connection = core_connection();

        manifest_write(&connection, FORMAT_VERSION_KEY, "1").unwrap();
        manifest_write(&connection, DATA_VERSION_KEY, "schema-a").unwrap();
        manifest_write(&connection, REVISION_SEQ_KEY, "42").unwrap();
        manifest_write(&connection, REVISION_SEQ_KEY, "43").unwrap();

        assert_eq!(manifest_read(&connection, FORMAT_VERSION_KEY).unwrap(), Some("1".to_string()));
        assert_eq!(manifest_read(&connection, DATA_VERSION_KEY).unwrap(), Some("schema-a".to_string()));
        assert_eq!(manifest_read(&connection, REVISION_SEQ_KEY).unwrap(), Some("43".to_string()));
        assert_eq!(manifest_read(&connection, RESET_INTENT_KEY).unwrap(), None);
        assert_eq!(fingerprint_key("message"), "fingerprint:message");
    }

    #[test]
    fn missing_format_format_mismatch_and_data_version_mismatch_are_incompatible() {
        let missing_format = verdict(&[("fingerprint:message", "message-v1")], None, &[]);
        let format_mismatch = verdict(&[(FORMAT_VERSION_KEY, "0")], None, &[]);
        let data_mismatch = verdict(&[(FORMAT_VERSION_KEY, "1"), (DATA_VERSION_KEY, "a")], Some("b"), &[]);

        assert_eq!(missing_format, ManifestVerdict::Incompatible);
        assert_eq!(format_mismatch, ManifestVerdict::Incompatible);
        assert_eq!(data_mismatch, ManifestVerdict::Incompatible);
    }

    #[test]
    fn empty_database_is_fresh_and_fingerprint_mismatch_touches_one_model() {
        let fresh = verdict(&[], None, &[]);
        let cache_only = verdict(
            &[(FORMAT_VERSION_KEY, "1"), ("fingerprint:message", "x"), ("fingerprint:chat", "y")],
            None,
            &[("message", "z"), ("chat", "y")],
        );
        let removed = verdict(&[(FORMAT_VERSION_KEY, "1"), ("fingerprint:gone", "x")], None, &[]);
        let added = verdict(&[(FORMAT_VERSION_KEY, "1")], None, &[("new", "x")]);

        assert_eq!(fresh, ManifestVerdict::Fresh);
        assert_eq!(cache_only, ManifestVerdict::CacheOnly { models: vec!["message".to_string()] });
        assert_eq!(removed, ManifestVerdict::CacheOnly { models: vec!["gone".to_string()] });
        assert_eq!(added, ManifestVerdict::Compatible);
    }

    #[test]
    fn empty_manifest_with_cached_rows_is_incompatible() {
        let connection = core_connection();
        connection.execute("insert into tombstone (model, id, seq, deleted_at) values ('m', '1', 1, 0)", []).unwrap();

        assert_eq!(manifest_verify(&connection, None, &BTreeMap::new()).unwrap(), ManifestVerdict::Incompatible);
    }

    #[test]
    fn stamp_makes_matching_manifest_compatible() {
        let connection = core_connection();
        let fingerprints = BTreeMap::from([("messages".to_string(), "fp1".to_string())]);

        manifest_stamp(&connection, Some("v1"), &fingerprints).unwrap();

        assert_eq!(manifest_verify(&connection, Some("v1"), &fingerprints).unwrap(), ManifestVerdict::Compatible);
        assert_eq!(manifest_verify(&connection, Some("v2"), &fingerprints).unwrap(), ManifestVerdict::Incompatible);
        assert_eq!(manifest_read(&connection, RECORD_VERSION_KEY).unwrap(), Some(RECORD_VERSION.to_string()));
    }

    #[test]
    fn repeated_stamp_does_not_add_manifest_rows() {
        let connection = core_connection();
        let fingerprints = BTreeMap::from([("messages".to_string(), "fp1".to_string())]);
        let count = |connection: &Connection| connection.query_row("select count(*) from manifest", [], |row| row.get::<_, i64>(0)).unwrap();

        manifest_stamp(&connection, Some("v1"), &fingerprints).unwrap();
        let first = count(&connection);
        manifest_stamp(&connection, Some("v1"), &fingerprints).unwrap();

        assert_eq!(count(&connection), first);
    }
}
