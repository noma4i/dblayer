use super::*;
use crate::identity::ListKey;
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
fn offset_without_limit_reads_to_the_end() {
    let statement = compile_keys(&query().offset_by(4), &people()).unwrap();
    assert_eq!(statement.sql, format!("{HEAD} LIMIT -1 OFFSET ?"));
    assert_eq!(statement.bindings, vec![SqlValue::Integer(4)]);
}

#[test]
fn unknown_order_field_and_foreign_model_are_rejected() {
    let unknown = compile_keys(&query().ordered(vec![Order::new("nope", SortDirection::Asc)]), &people());
    assert_eq!(unknown.unwrap_err(), QueryError::UnknownField("nope".into()));
    let foreign = compile_keys(&Query::new("groups"), &people());
    assert_eq!(foreign.unwrap_err(), QueryError::ModelMismatch { query: "groups".into(), model: "people".into() });
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
    let scope = ListKey::from_raw("feed");
    let filtered = query().in_scope(scope).filtered(Where::equal("status", json!("active")));
    let statement = compile_count(&filtered, &people()).unwrap();
    assert!(statement.sql.contains("JOIN scope_member"));
    assert!(statement.sql.contains("WHERE"));
    assert_eq!(statement.bindings.len(), 3);
    assert!(statement.is_balanced());
}
