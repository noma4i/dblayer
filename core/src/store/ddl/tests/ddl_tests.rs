use super::*;
use crate::schema::{RelationSpec, SortField};
use crate::value::{ScalarKind, SortDirection};
use rusqlite::Connection;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

fn fingerprint(model: &ModelSpec) -> String { super::fingerprint(model).unwrap() }

fn fingerprint_text(model: &ModelSpec) -> String { super::fingerprint_text(model).unwrap() }

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
fn index_names_distinguish_column_names_from_sort_terms() {
    let model = ModelSpec {
        relations: vec![RelationSpec { sort: Sort::Fields(vec![SortField { column: "position".into(), direction: SortDirection::Desc }]), ..RelationSpec::global("recent") }],
        ..ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("position", ScalarKind::Int).indexed(), FieldSpec::scalar("positionDescId", ScalarKind::Int).indexed()])
    };
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(&create_table_statement(&model)).unwrap();
    let statements = create_index_statements(&model);
    for sql in &statements { connection.execute_batch(sql).unwrap(); }
    let count: i64 = connection.query_row("SELECT count(*) FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL", [], |row| row.get(0)).unwrap();
    assert_eq!(count as usize, statements.len(), "an index name silently replaced a different definition: {statements:?}");
}

#[test]
fn inv_order_02_a_sorted_relation_index_carries_the_temp_row_flag_before_the_id() {
    let model = ModelSpec {
        relations: vec![RelationSpec { sort: Sort::Fields(vec![SortField { column: "position".into(), direction: SortDirection::Desc }]), ..RelationSpec::global("recent") }],
        ..ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("position", ScalarKind::Int)])
    };
    let statements = create_index_statements(&model);
    assert!(statements.iter().any(|sql| sql.ends_with("(\"position\" DESC, (substr(\"id\", 1, 5) <> 'temp-'), \"id\")")), "{statements:?}");
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(&create_table_statement(&model)).unwrap();
    for sql in &statements { connection.execute_batch(sql).unwrap(); }
}

#[test]
fn create_indexes_remove_duplicate_and_prefix_indexes() {
    let model = ModelSpec { indexes: vec![vec!["chatId".into(), "sequenceNumber".into()]; 2], ..messages() };
    let statements = create_index_statements(&model);

    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(&create_table_statement(&model)).unwrap();
    for sql in &statements { connection.execute_batch(sql).unwrap(); }
    assert_eq!(statements.len(), 4);
    assert!(!statements.iter().any(|sql| sql.ends_with("(\"chat_id\")")));
    assert!(statements.iter().any(|sql| sql.ends_with("(\"chat_id\", \"sequence_number\")")));
}

#[test]
fn model_statements_keep_model_order() {
    let specs = [groups(), messages()];
    let statements = models_statements(&specs);
    let groups_at = statements.iter().position(|sql| sql.contains("CREATE TABLE IF NOT EXISTS row_groups")).unwrap();
    let messages_at = statements.iter().position(|sql| sql.contains("CREATE TABLE IF NOT EXISTS row_messages")).unwrap();

    assert!(groups_at < messages_at);
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
    assert!(indexes.contains("row_messages_idx_0") && indexes.contains("row_groups_idx_0"));
}

#[test]
fn row_table_matches_spec_columns() {
    let connection = Connection::open_in_memory().unwrap();
    create_model_tables(&connection, &[messages()]).unwrap();
    let found = columns(&connection, "row_messages");

    assert_eq!(found["id"], ("TEXT".to_string(), true));
    assert_eq!(found["revision"], ("INTEGER".to_string(), true));
    assert_eq!(found["born_rev"], ("INTEGER".to_string(), true));
    assert_eq!(found["chat_id"], ("TEXT".to_string(), false));
    assert_eq!(found["sequence_number"], ("INTEGER".to_string(), false));
    assert_eq!(found["score"], ("REAL".to_string(), false));
    assert_eq!(found["created_at"], ("TEXT".to_string(), false));
    for rev in ["rev__chat_id", "rev__sequence_number", "rev__score", "rev__created_at", "rev__media"] {
        assert_eq!(found[rev], ("INTEGER".to_string(), false), "{rev}");
    }
    assert_eq!(found.len(), 12);
}

#[test]
fn integer_lands_in_an_indexed_column() {
    let connection = Connection::open_in_memory().unwrap();
    create_model_tables(&connection, &[messages()]).unwrap();

    connection
        .execute(
            "insert into row_messages (id, revision, born_rev, sequence_number) values (?1, 1, 1, ?2)",
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

fn refused(result: rusqlite::Result<usize>) -> i32 {
    match result {
        Err(rusqlite::Error::SqliteFailure(error, _)) => error.extended_code,
        other => panic!("not a refusal of SQLite: {other:?}"),
    }
}

#[test]
fn strict_row_table_rejects_wrong_column_type() {
    let connection = Connection::open_in_memory().unwrap();
    create_model_tables(&connection, &[messages()]).unwrap();

    let result = connection.execute(
        "insert into row_messages (id, revision, born_rev, sequence_number) values ('r', 1, 1, 'abc')",
        [],
    );

    assert_eq!(refused(result), rusqlite::ffi::SQLITE_CONSTRAINT_DATATYPE);
}

#[test]
fn core_migration_creates_tables_and_indexes() {
    let connection = core_connection();
    let tables: BTreeSet<String> =
        ["manifest", "scope_member", "tombstone", "query_window", "window_marker", "op", "quarantine", "row_sync"].map(String::from).into();
    let indexes: BTreeSet<String> = [
        "scope_walk",
        "scope_order_unique",
        "scope_member_row",
        "scope_member_place",
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
            "insert into op (action_key, status, created_at, since) values ('Chat:send', ?1, 0, 0)",
            [status],
        )
    };

    assert_eq!(refused(duplicate), rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE);
    assert_eq!(refused(op("unknown")), rusqlite::ffi::SQLITE_CONSTRAINT_CHECK);
    assert!(op("pending").is_ok());
}

#[test]
fn inv_op_10_open_dedupe_key_is_unique_only_while_open() {
    let connection = core_connection();
    let insert = |status: &str, dedupe: Option<&str>| {
        connection.execute(
            "insert into op (action_key, status, created_at, since, dedupe_key) values ('Chat:send', ?1, 0, 0, ?2)",
            rusqlite::params![status, dedupe],
        )
    };

    assert!(insert("pending", Some("k")).is_ok());
    assert_eq!(refused(insert("pending", Some("k"))), rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE);
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
        "status",
        "temp_id",
        "base_revision",
        "created_at",
        "dedupe_key",
        "client_mutation_id",
        "attempts",
        "cause_kind",
        "cause_limit",
        "cycle_id",
        "since",
        "error_kind",
        "error_message",
        "intent_state",
        "origin_kind",
        "transfer_id",
        "batch_id",
    ];
    assert_eq!(op_names, expected_op.into_iter().collect());
    assert!(!op["dedupe_key"].1 && !op["temp_id"].1 && !op["base_revision"].1 && !op["error_kind"].1);
    assert_eq!(window.len(), 11);
    assert!(window["landed_at"].1 && !window["cursor"].1 && !window["total"].1 && !window["stale_since"].1);
    let bad_flag = connection.execute(
        "insert into query_window (key, has_next, pages, issued, applied, generation, landed_at, stale_since) values ('k', 0, 1, 0, 0, 0, 0, 0)",
        [],
    );
    assert_eq!(refused(bad_flag), rusqlite::ffi::SQLITE_CONSTRAINT_CHECK);
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
fn prepared_connection_uses_normal_sync_no_foreign_keys_and_memory_temp_tables() {
    let connection = Connection::open_in_memory().unwrap();
    prepare_connection(&connection).unwrap();
    let pragma = |name: &str| -> i64 { connection.query_row(&format!("pragma {name}"), [], |row| row.get(0)).unwrap() };

    assert_eq!((pragma("synchronous"), pragma("foreign_keys"), pragma("temp_store")), (1, 0, 2));
}

#[test]
fn memory_models_get_temp_tables_with_their_indexes() {
    let model = ModelSpec { storage: crate::schema::Storage::Memory, ..messages() };
    let connection = core_connection();
    create_model_tables(&connection, &[model]).unwrap();
    let temp: i64 = connection.query_row("select count(*) from sqlite_temp_master where name = 'row_messages'", [], |row| row.get(0)).unwrap();
    let temp_indexes: i64 = connection.query_row("select count(*) from sqlite_temp_master where type = 'index' and tbl_name = 'row_messages'", [], |row| row.get(0)).unwrap();

    assert_eq!(temp, 1);
    assert!(temp_indexes >= 4);
    assert!(!names(&connection, "table").contains("row_messages"));
}

#[test]
fn ephemeral_fields_do_not_change_the_fingerprint() {
    let plain = fingerprint(&messages());
    let with_ephemeral = ModelSpec {
        ephemeral: vec![crate::schema::EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false)))],
        ..messages()
    };

    assert_eq!(fingerprint(&with_ephemeral), plain);
}

#[test]
fn associations_and_storage_change_the_fingerprint() {
    let plain = fingerprint(&messages());
    let mut with_association = messages();
    with_association.associations.push(crate::schema::AssociationSpec::new("author", crate::schema::AssociationKind::BelongsTo, "users", "authorId"));
    let mut local = messages();
    local.storage = crate::schema::Storage::Local;

    assert_ne!(fingerprint(&with_association), plain);
    assert_ne!(fingerprint(&local), plain);
}

#[test]
fn a_relation_without_row_rules_keeps_the_fingerprint_text_it_had_before_row_rules_existed() {
    let plain = ModelSpec { relations: vec![RelationSpec::new("list", &["chatId"])], ..messages() };
    let mut ruled = plain.clone();
    ruled.relations[0].row_rules = vec![crate::schema::RowRule { field: "kind".into(), test: crate::schema::RowTest::Equals { value: json!("text") } }];

    assert!(!fingerprint_text(&plain).contains("row_rules"));
    assert_ne!(fingerprint(&ruled), fingerprint(&plain));
}

#[test]
fn inv_dsl_04_fingerprint_ignores_declaration_order() {
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
fn data_orders_fingerprint_over_every_declaration_order() {
    use crate::test_wait::permutations;
    let fields = vec![
        FieldSpec::scalar("title", ScalarKind::Str).nullable(),
        FieldSpec::scalar("count", ScalarKind::Int).indexed().with_default(json!(0)),
        FieldSpec::scalar("createdAt", ScalarKind::Date),
    ];
    let relations = vec![("author", vec!["tenantId", "authorId"]), ("comments", vec!["postId", "position"])];
    let build = |fields: Vec<FieldSpec>, relations: Vec<(&str, Vec<&str>)>| ModelSpec { relations: relations.into_iter().map(|(name, keys)| RelationSpec::new(name, &keys)).collect(), ..ModelSpec::new("posts", fields) };
    let expected = fingerprint(&build(fields.clone(), relations.clone()));
    let mut traces = 0;
    for field_order in permutations(&fields) {
        for relation_order in permutations(&relations) {
            for author_keys in permutations(&relations[0].1) {
                for comment_keys in permutations(&relations[1].1) {
                    let relations = relation_order.iter().map(|(name, _)| match *name { "author" => (*name, author_keys.clone()), _ => (*name, comment_keys.clone()) }).collect::<Vec<_>>();
                    assert_eq!(fingerprint(&build(field_order.clone(), relations)), expected, "{field_order:?} {relation_order:?} {author_keys:?} {comment_keys:?}");
                    traces += 1;
                }
            }
        }
    }
    assert_eq!(traces, 6 * 2 * 2 * 2);
}

#[test]
fn inv_dsl_04_field_and_relation_changes_change_the_fingerprint() {
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
fn membership_changes_and_swapped_bindings_change_the_fingerprint() {
    use crate::schema::Membership;
    let mut model = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str), FieldSpec::scalar("status", ScalarKind::Str), FieldSpec::list("lists", ScalarKind::Str)]);
    let mut relation = RelationSpec::new("list", &[]);
    relation.text_params = vec!["first".into(), "second".into()];
    relation.membership = BTreeMap::from([("first".into(), Membership::Equals("title".into())), ("second".into(), Membership::Equals("status".into()))]);
    model.relations.push(relation);
    let original = fingerprint(&model);
    let mut changed = model.clone();
    changed.relations[0].membership.insert("first".into(), Membership::Contains("lists".into()));
    assert_ne!(original, fingerprint(&changed));
    let mut swapped = model.clone();
    swapped.relations[0].membership = BTreeMap::from([("first".into(), Membership::Equals("status".into())), ("second".into(), Membership::Equals("title".into()))]);
    assert_ne!(original, fingerprint(&swapped));
    let mut node = model;
    node.relations[0].joins_node = true;
    assert_ne!(original, fingerprint(&node));
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
    assert_eq!(fingerprint_key("message"), "fingerprint:message");
}

#[test]
fn missing_format_format_mismatch_and_data_version_mismatch_are_mismatched() {
    let missing_format = verdict(&[("fingerprint:message", "message-v1")], None, &[]);
    let format_mismatch = verdict(&[(FORMAT_VERSION_KEY, "0")], None, &[]);
    let format = format_version();
    let data_mismatch = verdict(&[(FORMAT_VERSION_KEY, format.as_str()), (DATA_VERSION_KEY, "a")], Some("b"), &[]);

    assert_eq!(missing_format, ManifestVerdict::Mismatch);
    assert_eq!(format_mismatch, ManifestVerdict::Mismatch);
    assert_eq!(data_mismatch, ManifestVerdict::Mismatch);
}

#[test]
fn empty_database_is_fresh_and_fingerprint_mismatch_touches_one_model() {
    let format = format_version();
    let fresh = verdict(&[], None, &[]);
    let cache_only = verdict(
        &[(FORMAT_VERSION_KEY, format.as_str()), ("fingerprint:message", "x"), ("fingerprint:chat", "y")],
        None,
        &[("message", "z"), ("chat", "y")],
    );
    let removed = verdict(&[(FORMAT_VERSION_KEY, format.as_str()), ("fingerprint:gone", "x")], None, &[]);
    let added = verdict(&[(FORMAT_VERSION_KEY, format.as_str())], None, &[("new", "x")]);

    assert_eq!(fresh, ManifestVerdict::Fresh);
    assert_eq!(cache_only, ManifestVerdict::CacheOnly { models: vec!["message".to_string()] });
    assert_eq!(removed, ManifestVerdict::CacheOnly { models: vec!["gone".to_string()] });
    assert_eq!(added, ManifestVerdict::Match);
}

#[test]
fn empty_manifest_with_cached_rows_is_mismatched() {
    let connection = core_connection();
    connection.execute("insert into tombstone (model, id, seq, deleted_at) values ('m', '1', 1, 0)", []).unwrap();

    assert_eq!(manifest_verify(&connection, None, &BTreeMap::new()).unwrap(), ManifestVerdict::Mismatch);
}

#[test]
fn stamp_makes_matching_manifest_matching() {
    let connection = core_connection();
    let fingerprints = BTreeMap::from([("messages".to_string(), "fp1".to_string())]);

    manifest_stamp(&connection, Some("v1"), &fingerprints).unwrap();

    assert_eq!(manifest_verify(&connection, Some("v1"), &fingerprints).unwrap(), ManifestVerdict::Match);
    assert_eq!(manifest_verify(&connection, Some("v2"), &fingerprints).unwrap(), ManifestVerdict::Mismatch);
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

#[test]
fn core_migration_creates_the_file_table_with_a_unique_pair_index() {
    let connection = core_connection();
    assert!(names(&connection, "table").contains("file"));
    assert!(names(&connection, "index").contains("file_pair"));
    let insert = "insert into file (id, model, row_id, field, name, mime, size, sha256, location, data, created_at) values (?1, 'moments', 'm1', 'media', 'a.png', 'image/png', 3, 'x', ?2, x'010203', 1)";
    connection.execute(insert, ["f1", "inline"]).unwrap();
    assert_eq!(refused(connection.execute(insert, ["f2", "inline"])), rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE);
    assert_eq!(refused(connection.execute(insert, ["f3", "elsewhere"])), rusqlite::ffi::SQLITE_CONSTRAINT_CHECK);
}
