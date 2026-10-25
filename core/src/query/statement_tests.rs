use super::*;
use crate::identity::ScopeKey;
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

const HEAD: &str = "SELECT r.id, r.revision FROM row_people AS r";

#[test]
fn bare_query_reads_the_whole_table() {
    let statement = compile_keys(&query(), &people()).unwrap();
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
    let statement = compile_keys(&query, &people()).unwrap();
    assert_eq!(
        statement.sql,
        format!("{HEAD} WHERE r.\"status\" = ? ORDER BY r.\"created_at\" DESC, r.id LIMIT ? OFFSET ?")
    );
    assert_eq!(
        statement.bindings,
        vec![SqlValue::Text("active".into()), SqlValue::Integer(25), SqlValue::Integer(50)]
    );
    assert!(statement.is_balanced());
}

#[test]
fn offset_without_limit_reads_to_the_end() {
    let statement = compile_keys(&query().offset_by(4), &people()).unwrap();
    assert_eq!(statement.sql, format!("{HEAD} LIMIT -1 OFFSET ?"));
    assert_eq!(statement.bindings, vec![SqlValue::Integer(4)]);
}

#[test]
fn id_order_is_not_repeated_as_tie_break() {
    let statement = compile_keys(&query().ordered(vec![Order::new("id", SortDirection::Desc)]), &people()).unwrap();
    assert_eq!(statement.sql, format!("{HEAD} ORDER BY r.id DESC"));
}

#[test]
fn plain_order_uses_its_column() {
    let statement = compile_keys(&query().ordered(vec![Order::new("kind", SortDirection::Asc)]), &people()).unwrap();
    assert_eq!(statement.sql, format!("{HEAD} ORDER BY r.\"kind\" ASC, r.id"));
}

#[test]
fn unknown_order_field_and_foreign_model_are_rejected() {
    let unknown = compile_keys(&query().ordered(vec![Order::new("nope", SortDirection::Asc)]), &people());
    assert_eq!(unknown.unwrap_err(), QueryError::UnknownField("nope".into()));
    let foreign = compile_keys(&Query::new("groups"), &people());
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
    assert_eq!(statement.sql, "SELECT COUNT(*) FROM row_people AS r WHERE r.\"status\" = ?");
    assert_eq!(statement.bindings, vec![SqlValue::Text("active".into())]);
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
fn a_scope_query_keeps_its_filter_and_counts_members() {
    let scope = ScopeKey::from_raw("feed");
    let filtered = query().in_scope(scope).filtered(Where::equal("status", json!("active")));
    let statement = compile_count(&filtered, &people()).unwrap();
    assert!(statement.sql.contains("JOIN scope_member"));
    assert!(statement.sql.contains("WHERE"));
    assert_eq!(statement.bindings.len(), 3);
    assert!(statement.is_balanced());
}
