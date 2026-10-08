use super::*;
use crate::ddl::{create_model_tables, migrate_core, RECORD_VERSION};
use crate::identity::{ModelKey, ScopeKey};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::{null_compare, null_equals, null_is_in, null_is_not_in, null_not_equals, ScalarKind, SortDirection};
use rusqlite::{params, Connection};
use serde_json::{json, Map, Value};

fn people() -> ModelSpec {
    ModelSpec::new(
        "people",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("status", ScalarKind::Str).nullable().indexed(),
            FieldSpec::scalar("age", ScalarKind::Int).nullable().indexed(),
            FieldSpec::scalar("enabled", ScalarKind::Bool).nullable().indexed(),
            FieldSpec::scalar("createdAt", ScalarKind::Date).indexed(),
            FieldSpec::scalar("kind", ScalarKind::Str).nullable(),
        ],
    )
}

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    create_model_tables(&connection, &[people()]).unwrap();
    connection
}

fn put_versioned(connection: &Connection, row: Value, record_version: i64, payload_id: &str) {
    let mut payload = row.as_object().unwrap().clone();
    let id = payload["id"].as_str().unwrap().to_string();
    payload.insert("id".into(), json!(payload_id));
    let text = |name: &str| payload.get(name).and_then(Value::as_str).map(str::to_string);
    connection
        .execute(
            "INSERT INTO row_people (id, revision, born_rev, field_rev, record_version, payload, status, age, enabled, created_at) \
             VALUES (?1, 1, 1, x'', ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id,
                record_version,
                serde_json::to_vec(&payload).unwrap(),
                text("status"),
                payload.get("age").and_then(Value::as_i64),
                payload.get("enabled").and_then(Value::as_bool),
                text("createdAt"),
            ],
        )
        .unwrap();
}

fn put(connection: &Connection, row: Value) {
    let id = row["id"].as_str().unwrap().to_string();
    put_versioned(connection, row, RECORD_VERSION, &id);
}

fn sample(id: &str, status: Value, age: Value, enabled: Value, kind: Value, created: &str) -> Value {
    json!({"id": id, "status": status, "age": age, "enabled": enabled, "kind": kind, "createdAt": created})
}

fn seeded() -> Connection {
    let connection = database();
    put(&connection, sample("p1", json!("active"), json!(30), json!(true), json!("chat"), "2024-01-01"));
    put(&connection, sample("p2", json!("active"), json!(17), json!(false), json!("call"), "2024-01-03"));
    put(&connection, sample("p3", json!("banned"), json!(null), json!(null), json!(null), "2024-01-02"));
    put(&connection, sample("p4", json!(null), json!(18), json!(true), json!("chat"), "2024-01-05"));
    put(&connection, sample("p5", json!("idle"), json!(45), json!(false), json!("note"), "2024-01-04"));
    put(&connection, sample("p6", json!(null), json!(null), json!(null), json!(null), "2024-01-06"));
    connection
}

fn ids(read: &RowsRead) -> Vec<String> {
    read.rows.iter().map(|row| row["id"].as_str().unwrap().to_string()).collect()
}

fn query_ids(connection: &Connection, query: &Query) -> Vec<String> {
    let statement = compile(query, &people()).unwrap();
    ids(&fetch_rows(connection, &statement).unwrap())
}

fn matches(clause: &Where, row: &Map<String, Value>) -> bool {
    match clause {
        Where::Equal { field, value } => null_equals(row.get(field), Some(value)),
        Where::NotEqual { field, value } => null_not_equals(row.get(field), Some(value)),
        Where::Compare { field, op, value } => {
            let (keeps_greater, or_equal) = match op {
                CompareOp::Less => (false, false),
                CompareOp::LessOrEqual => (false, true),
                CompareOp::Greater => (true, false),
                CompareOp::GreaterOrEqual => (true, true),
            };
            null_compare(row.get(field), value, keeps_greater, or_equal)
        }
        Where::InList { field, values } => null_is_in(row.get(field), values),
        Where::NotInList { field, values } => null_is_not_in(row.get(field), values),
        Where::IsNull { field } => row.get(field).is_none_or(Value::is_null),
        Where::AllOf { clauses } => clauses.iter().all(|clause| matches(clause, row)),
        Where::AnyOf { clauses } => clauses.iter().any(|clause| matches(clause, row)),
        Where::Not { clause } => !matches(clause, row),
    }
}

fn clauses() -> Vec<Where> {
    let greater = |field: &str, bound: i64| Where::compare(field, CompareOp::Greater, json!(bound));
    vec![
        Where::equal("status", json!("active")),
        Where::equal("status", json!(null)),
        Where::not_equal("status", json!("active")),
        Where::not_equal("status", json!(null)),
        Where::equal("kind", json!("chat")),
        Where::not_equal("kind", json!("chat")),
        Where::equal("enabled", json!(true)),
        Where::negated(Where::equal("enabled", json!(true))),
        Where::compare("age", CompareOp::Less, json!(18)),
        Where::compare("age", CompareOp::LessOrEqual, json!(18)),
        Where::compare("age", CompareOp::GreaterOrEqual, json!(18)),
        Where::negated(greater("age", 20)),
        Where::in_list("status", vec![json!("active"), json!("idle")]),
        Where::in_list("status", vec![json!("active"), json!(null)]),
        Where::not_in_list("status", vec![json!("active")]),
        Where::not_in_list("status", vec![json!("active"), json!(null)]),
        Where::in_list("status", vec![]),
        Where::not_in_list("status", vec![]),
        Where::in_list("status", vec![json!(null)]),
        Where::not_in_list("status", vec![json!(null)]),
        Where::not_in_list("kind", vec![json!("chat"), json!("call")]),
        Where::negated(Where::in_list("status", vec![json!("active")])),
        Where::is_null("age"),
        Where::negated(Where::is_null("age")),
        Where::all_of(vec![Where::equal("status", json!("active")), greater("age", 20)]),
        Where::any_of(vec![Where::equal("kind", json!("note")), Where::is_null("status")]),
        Where::negated(Where::all_of(vec![greater("age", 20), Where::equal("enabled", json!(true))])),
        Where::negated(Where::any_of(vec![greater("age", 20), Where::equal("kind", json!("chat"))])),
        Where::all_of(vec![]),
        Where::any_of(vec![]),
    ]
}

#[test]
fn sql_selects_the_same_rows_as_the_row_predicate() {
    let connection = seeded();
    let everything = fetch_rows(&connection, &compile(&Query::new("people"), &people()).unwrap()).unwrap();
    let mut dividing = 0;
    for clause in clauses() {
        let expected: Vec<String> = everything
            .rows
            .iter()
            .filter(|row| matches(&clause, row))
            .map(|row| row["id"].as_str().unwrap().to_string())
            .collect();
        let mut actual = query_ids(&connection, &Query::new("people").filtered(clause.clone()));
        actual.sort();
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        assert_eq!(actual, expected_sorted, "clause {clause:?}");
        if !expected.is_empty() && expected.len() < everything.rows.len() {
            dividing += 1;
        }
    }
    assert!(dividing >= 18, "only {dividing} clauses divide the sample");
}

#[test]
fn json_extract_reads_the_blob_payload() {
    let connection = seeded();
    let query = Query::new("people").filtered(Where::equal("kind", json!("chat")));
    assert_eq!(query_ids(&connection, &query).len(), 2);
}

#[test]
fn promoted_comparison_uses_the_column_index() {
    let connection = seeded();
    let statement = compile(&Query::new("people").filtered(Where::equal("status", json!("active"))), &people()).unwrap();
    let plan: String = connection
        .query_row(&format!("EXPLAIN QUERY PLAN {}", statement.sql), rusqlite::params_from_iter(statement.bindings.iter()), |row| {
            row.get(3)
        })
        .unwrap();
    assert!(plan.contains("USING INDEX row_people_status"), "{plan}");
}

#[test]
fn order_limit_and_offset_page_through_rows() {
    let connection = seeded();
    let newest_first = Query::new("people").ordered(vec![Order::new("createdAt", SortDirection::Desc)]);
    assert_eq!(query_ids(&connection, &newest_first), ["p6", "p4", "p5", "p2", "p3", "p1"]);
    assert_eq!(query_ids(&connection, &newest_first.clone().limited(2)), ["p6", "p4"]);
    assert_eq!(query_ids(&connection, &newest_first.clone().limited(2).offset_by(2)), ["p5", "p2"]);
    assert_eq!(query_ids(&connection, &newest_first.offset_by(4)), ["p3", "p1"]);
}

#[test]
fn ties_break_by_id() {
    let connection = seeded();
    let by_status = Query::new("people").ordered(vec![Order::new("status", SortDirection::Asc)]);
    assert_eq!(query_ids(&connection, &by_status), ["p4", "p6", "p1", "p2", "p3", "p5"]);
}

#[test]
fn count_ignores_limit_and_offset() {
    let connection = seeded();
    let query = Query::new("people").filtered(Where::equal("status", json!("active"))).limited(1).offset_by(1);
    assert_eq!(fetch_count(&connection, &compile_count(&query, &people()).unwrap()).unwrap(), 2);
    assert_eq!(fetch_count(&connection, &compile_count(&Query::new("people"), &people()).unwrap()).unwrap(), 6);
}

#[test]
fn stale_and_unreadable_records_are_counted_not_returned() {
    let connection = database();
    put(&connection, sample("ok", json!("a"), json!(1), json!(true), json!("k"), "2024-01-01"));
    put_versioned(&connection, sample("old", json!("a"), json!(1), json!(true), json!("k"), "2024-01-02"), RECORD_VERSION + 1, "old");
    put_versioned(&connection, sample("liar", json!("a"), json!(1), json!(true), json!("k"), "2024-01-03"), RECORD_VERSION, "other");
    let read = fetch_rows(&connection, &compile(&Query::new("people"), &people()).unwrap()).unwrap();
    assert_eq!(ids(&read), ["ok"]);
    assert_eq!((read.stale_version_drops, read.rejected_records), (1, 1));
}

fn member(connection: &Connection, scope: &str, id: &str, order_key: &str) {
    connection
        .execute("INSERT INTO scope_member (model, scope_key, row_id, order_key) VALUES ('people', ?1, ?2, ?3)", params![scope, id, order_key])
        .unwrap();
}

#[test]
fn scope_read_follows_order_key_and_skips_dangling_members() {
    let connection = seeded();
    member(&connection, "inbox", "p1", "b");
    member(&connection, "inbox", "p2", "a");
    member(&connection, "inbox", "ghost", "c");
    member(&connection, "inbox", "p3", "d");
    member(&connection, "other", "p4", "a");
    let model = ModelKey::new("people");
    let scope = ScopeKey::from_raw("inbox");
    let all = fetch_rows(&connection, &scope_statement(&model, &scope, None)).unwrap();
    assert_eq!(ids(&all), ["p2", "p1", "p3"]);
    let windowed = fetch_rows(&connection, &scope_statement(&model, &scope, Some(2))).unwrap();
    assert_eq!(ids(&windowed), ["p2", "p1"]);
    assert_eq!(fetch_count(&connection, &scope_count_statement(&model, &scope)).unwrap(), 3);
}

#[test]
fn scope_read_plan_walks_the_scope_index_without_sorting() {
    let connection = seeded();
    let statement = scope_statement(&ModelKey::new("people"), &ScopeKey::from_raw("inbox"), Some(5));
    let mut prepared = connection.prepare(&format!("EXPLAIN QUERY PLAN {}", statement.sql)).unwrap();
    let details: Vec<String> = prepared
        .query_map(rusqlite::params_from_iter(statement.bindings.iter()), |row| row.get::<_, String>(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(details.iter().any(|detail| detail.contains("scope_walk")), "{details:?}");
    assert!(details.iter().all(|detail| !detail.contains("TEMP B-TREE")), "{details:?}");
}

#[test]
fn fetch_keys_returns_ids_and_revisions_in_row_order() {
    let connection = seeded();
    connection.execute("UPDATE row_people SET revision = 7 WHERE id = 'p2'", []).unwrap();
    let query = Query::new("people").filtered(Where::equal("status", json!("active"))).ordered(vec![Order::new("createdAt", SortDirection::Desc)]);
    let statement = compile_keys(&query, &people()).unwrap();
    assert_eq!(fetch_keys(&connection, &statement).unwrap(), vec![("p2".to_string(), 7), ("p1".to_string(), 1)]);
}
