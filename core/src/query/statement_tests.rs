use super::*;
use crate::identity::{ModelKey, ScopeKey};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::{ScalarKind, SortDirection};
use rusqlite::types::Value as SqlValue;
use serde_json::json;

fn people() -> ModelSpec {
    ModelSpec::new(
        "people",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("status", ScalarKind::Str).nullable().indexed(),
            FieldSpec::scalar("createdAt", ScalarKind::Date).indexed(),
            FieldSpec::scalar("kind", ScalarKind::Str),
        ],
    )
}

fn query() -> Query {
    Query::new("people")
}

const HEAD: &str = "SELECT r.id, r.record_version, r.payload FROM row_people AS r";

#[test]
fn bare_query_reads_the_whole_table() {
    let statement = compile(&query(), &people()).unwrap();
    assert_eq!(statement.sql, HEAD);
    assert!(statement.bindings.is_empty());
}

#[test]
fn query_composes_where_order_limit_and_offset() {
    let query = query()
        .filtered(Where::equal("status", json!("active")))
        .ordered(vec![Order::new("createdAt", SortDirection::Desc)])
        .limited(25)
        .offset_by(50);
    let statement = compile(&query, &people()).unwrap();
    assert_eq!(
        statement.sql,
        format!("{HEAD} WHERE r.status = ? ORDER BY r.created_at DESC, r.id LIMIT ? OFFSET ?")
    );
    assert_eq!(
        statement.bindings,
        vec![SqlValue::Text("active".into()), SqlValue::Integer(25), SqlValue::Integer(50)]
    );
    assert!(statement.is_balanced());
}

#[test]
fn clauses_are_added_one_at_a_time() {
    let filtered = compile(&query().filtered(Where::equal("status", json!("a"))), &people()).unwrap();
    assert_eq!(filtered.sql, format!("{HEAD} WHERE r.status = ?"));
    let ordered = compile(&query().ordered(vec![Order::new("status", SortDirection::Asc)]), &people()).unwrap();
    assert_eq!(ordered.sql, format!("{HEAD} ORDER BY r.status ASC, r.id"));
    let limited = compile(&query().limited(3), &people()).unwrap();
    assert_eq!(limited.sql, format!("{HEAD} LIMIT ?"));
    assert_eq!(limited.bindings, vec![SqlValue::Integer(3)]);
}

#[test]
fn offset_without_limit_reads_to_the_end() {
    let statement = compile(&query().offset_by(4), &people()).unwrap();
    assert_eq!(statement.sql, format!("{HEAD} LIMIT -1 OFFSET ?"));
    assert_eq!(statement.bindings, vec![SqlValue::Integer(4)]);
}

#[test]
fn id_order_is_not_repeated_as_tie_break() {
    let statement = compile(&query().ordered(vec![Order::new("id", SortDirection::Desc)]), &people()).unwrap();
    assert_eq!(statement.sql, format!("{HEAD} ORDER BY r.id DESC"));
}

#[test]
fn unpromoted_order_uses_json_extract() {
    let statement = compile(&query().ordered(vec![Order::new("kind", SortDirection::Asc)]), &people()).unwrap();
    assert_eq!(statement.sql, format!("{HEAD} ORDER BY json_extract(r.payload, '$.kind') ASC, r.id"));
}

#[test]
fn unknown_order_field_and_foreign_model_are_rejected() {
    let unknown = compile(&query().ordered(vec![Order::new("nope", SortDirection::Asc)]), &people());
    assert_eq!(unknown.unwrap_err(), QueryError::UnknownField("nope".into()));
    let foreign = compile(&Query::new("groups"), &people());
    assert_eq!(foreign.unwrap_err(), QueryError::ModelMismatch { query: "groups".into(), model: "people".into() });
}

#[test]
fn count_ignores_order_limit_and_offset() {
    let query = query()
        .filtered(Where::equal("status", json!("active")))
        .ordered(vec![Order::new("createdAt", SortDirection::Desc)])
        .limited(5)
        .offset_by(1);
    let statement = compile_count(&query, &people()).unwrap();
    assert_eq!(statement.sql, "SELECT COUNT(*) FROM row_people AS r WHERE r.status = ?");
    assert_eq!(statement.bindings, vec![SqlValue::Text("active".into())]);
}

#[test]
fn scope_read_and_count_match_the_documented_sql() {
    let model = ModelKey::new("message");
    let scope = ScopeKey::from_raw("inbox");
    let base = "SELECT r.id, r.record_version, r.payload FROM scope_member AS m JOIN row_message AS r \
                ON r.id = m.row_id WHERE m.model = ? AND m.scope_key = ? ORDER BY m.order_key, m.row_id";
    let unlimited = scope_statement(&model, &scope, None);
    assert_eq!(unlimited.sql, base);
    assert_eq!(unlimited.bindings, vec![SqlValue::Text("message".into()), SqlValue::Text("inbox".into())]);
    let limited = scope_statement(&model, &scope, Some(10));
    assert_eq!(limited.sql, format!("{base} LIMIT ?"));
    assert_eq!(limited.bindings.last(), Some(&SqlValue::Integer(10)));
    let count = scope_count_statement(&model, &scope);
    assert_eq!(
        count.sql,
        "SELECT COUNT(*) FROM scope_member AS m JOIN row_message AS r ON r.id = m.row_id \
         WHERE m.model = ? AND m.scope_key = ?"
    );
    assert!(!count.sql.contains("ORDER BY") && !count.sql.contains("LIMIT"));
    assert_eq!(count.bindings, vec![SqlValue::Text("message".into()), SqlValue::Text("inbox".into())]);
}

#[test]
fn table_name_follows_the_model_key() {
    let statement = scope_statement(&ModelKey::new("user-counters"), &ScopeKey::from_raw("s"), None);
    assert!(statement.sql.contains("row_user_counters AS r"));
}

#[test]
fn balance_compares_placeholders_with_bindings() {
    let balanced = CompiledStatement { sql: "SELECT 1 WHERE a = ? AND b = ?".into(), bindings: vec![SqlValue::Null, SqlValue::Null] };
    let unbalanced = CompiledStatement { sql: "SELECT 1 WHERE a = ? AND b = ?".into(), bindings: vec![SqlValue::Null] };
    assert_eq!(balanced.placeholder_count(), 2);
    assert!(balanced.is_balanced());
    assert!(!unbalanced.is_balanced());
    assert_eq!(CompiledStatement { sql: "SELECT 1".into(), bindings: vec![] }.placeholder_count(), 0);
}

#[test]
fn unindexed_fields_list_each_scan_once_in_order() {
    let query = query()
        .filtered(Where::all_of(vec![
            Where::equal("kind", json!("a")),
            Where::equal("status", json!("b")),
            Where::not_equal("kind", json!("c")),
            Where::equal("other", json!(1)),
        ]))
        .ordered(vec![Order::new("kind", SortDirection::Asc), Order::new("createdAt", SortDirection::Asc)]);
    assert_eq!(query.unindexed_fields(&people()), vec!["kind", "other"]);
    assert!(!query.is_index_only(&people()));
    let indexed = Query::new("people").filtered(Where::equal("status", json!("b")));
    assert!(indexed.unindexed_fields(&people()).is_empty());
    assert!(indexed.is_index_only(&people()));
    assert!(Query::new("people").is_index_only(&people()));
}

#[test]
fn query_round_trips_through_json() {
    let text = r#"{"model":"people","where":{"type":"equal","field":"status","value":"on"},"order":[{"field":"createdAt","direction":"desc"}],"limit":5,"offset":2}"#;
    let parsed: Query = serde_json::from_str(text).unwrap();
    let expected = query()
        .filtered(Where::equal("status", json!("on")))
        .ordered(vec![Order::new("createdAt", SortDirection::Desc)])
        .limited(5)
        .offset_by(2);
    assert_eq!(parsed, expected);
    assert_eq!(serde_json::from_str::<Query>(&serde_json::to_string(&parsed).unwrap()).unwrap(), parsed);
    assert_eq!(serde_json::from_str::<Query>(r#"{"model":"people"}"#).unwrap(), query());
}

#[test]
fn key_statement_reads_id_and_revision_with_the_same_clauses() {
    let query = query()
        .filtered(Where::equal("status", json!("active")))
        .ordered(vec![Order::new("createdAt", SortDirection::Desc)])
        .limited(25)
        .offset_by(50);
    let keys = compile_keys(&query, &people()).unwrap();
    let rows = compile(&query, &people()).unwrap();
    assert_eq!(keys.sql, rows.sql.replacen("r.id, r.record_version, r.payload", "r.id, r.revision", 1));
    assert_eq!(keys.bindings, rows.bindings);
}
