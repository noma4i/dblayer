use super::*;
use crate::query::statement::QueryError;
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use rusqlite::types::Value as SqlValue;
use serde_json::json;

fn people() -> ModelSpec {
    ModelSpec::new(
        "people",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("status", ScalarKind::Str).nullable().indexed(),
            FieldSpec::scalar("age", ScalarKind::Int).nullable().indexed(),
            FieldSpec::scalar("enabled", ScalarKind::Bool).nullable().indexed(),
            FieldSpec::scalar("kind", ScalarKind::Str),
            FieldSpec::object("media").nullable(),
        ],
    )
}

fn compiled(clause: &Where) -> (String, Vec<SqlValue>) {
    let mut bindings = Vec::new();
    let sql = clause.compile(&people(), &mut bindings).unwrap();
    (sql, bindings)
}

fn fault(clause: &Where) -> QueryError {
    clause.compile(&people(), &mut Vec::new()).unwrap_err()
}

#[test]
fn plain_fields_use_their_column() {
    assert_eq!(compiled(&Where::equal("kind", json!("chat"))), ("r.\"kind\" = ?".to_string(), vec![SqlValue::Text("chat".into())]));
    let mixed = Where::all_of(vec![Where::equal("chatId", json!("c1")), Where::compare("kind", CompareOp::Greater, json!("a"))]);
    assert_eq!(compiled(&mixed).0, "(r.\"chat_id\" = ? AND r.\"kind\" > ?)");
}

#[test]
fn id_is_always_a_column() {
    assert_eq!(compiled(&Where::equal("id", json!("m1"))).0, "r.id = ?");
}

#[test]
fn values_bind_by_scalar_kind() {
    let cases = [
        (json!("a"), SqlValue::Text("a".into())),
        (json!(true), SqlValue::Integer(1)),
        (json!(false), SqlValue::Integer(0)),
        (json!(3), SqlValue::Integer(3)),
        (json!(3.0), SqlValue::Integer(3)),
        (json!(0.5), SqlValue::Real(0.5)),
    ];
    for (value, expected) in cases {
        assert_eq!(compiled(&Where::equal("age", value)).1, vec![expected]);
    }
}

#[test]
fn unknown_field_and_container_values_are_rejected() {
    assert_eq!(fault(&Where::equal("missing", json!(1))), QueryError::UnknownField("missing".into()));
    assert_eq!(fault(&Where::equal("media", json!({}))), QueryError::UnsupportedValue("media".into()));
    assert_eq!(fault(&Where::in_list("age", vec![json!([1])])), QueryError::UnsupportedValue("age".into()));
}

#[test]
fn ephemeral_fields_cannot_be_filtered_or_ordered() {
    let model = ModelSpec { ephemeral: vec![crate::schema::EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false)))], ..people() };
    let rejected = Where::equal("typing", json!(true)).compile(&model, &mut Vec::new()).unwrap_err();
    assert_eq!(rejected, QueryError::EphemeralField("typing".into()));
    let ordered = crate::query::Query::new("people").ordered(vec![crate::query::Order::new("typing", crate::value::SortDirection::Asc)]);
    assert_eq!(crate::query::compile_keys(&ordered, &model).unwrap_err(), QueryError::EphemeralField("typing".into()));
}

#[test]
fn fields_walk_the_tree_and_null_safety_follows_lists() {
    let clause = Where::all_of(vec![
        Where::equal("first", json!(null)),
        Where::any_of(vec![Where::in_list("second", vec![]), Where::negated(Where::compare("third", CompareOp::Greater, json!(3)))]),
        Where::not_equal("first", json!("repeat")),
    ]);
    assert_eq!(clause.fields(), vec!["first", "second", "third", "first"]);
    assert!(Where::all_of(vec![]).fields().is_empty());
    let with_null = Where::all_of(vec![Where::in_list("id", vec![json!("one"), json!(null)]), Where::equal("kind", json!("item"))]);
    let without_null = Where::any_of(vec![
        Where::in_list("id", vec![json!("one")]),
        Where::negated(Where::equal("kind", json!("item"))),
    ]);
    assert!(!with_null.is_null_safe());
    assert!(without_null.is_null_safe());
}

#[test]
fn clause_round_trips_through_json() {
    let text = r#"{"type":"all_of","clauses":[{"type":"equal","field":"status","value":"on"},{"type":"not","clause":{"type":"is_null","field":"age"}},{"type":"compare","field":"age","op":"greater_or_equal","value":3},{"type":"not_in_list","field":"kind","values":["a"]}]}"#;
    let clause: Where = serde_json::from_str(text).unwrap();
    assert_eq!(
        clause,
        Where::all_of(vec![
            Where::equal("status", json!("on")),
            Where::negated(Where::is_null("age")),
            Where::compare("age", CompareOp::GreaterOrEqual, json!(3)),
            Where::not_in_list("kind", vec![json!("a")]),
        ])
    );
    assert_eq!(serde_json::from_str::<Where>(&serde_json::to_string(&clause).unwrap()).unwrap(), clause);
    let missing_value: Where = serde_json::from_str(r#"{"type":"equal","field":"status"}"#).unwrap();
    assert_eq!(missing_value, Where::equal("status", json!(null)));
}

#[test]
fn data_search_words() {
    use super::filter::fts_expression;
    assert_eq!(fts_expression("red apple pie", false).as_deref(), Some("\"red\" \"apple\" \"pie\"*"), "every word whole, the last a prefix");
    assert_eq!(fts_expression("red apple", true).as_deref(), Some("\"red\" \"apple\""), "whole words: every word whole");
    assert_eq!(fts_expression("a\"b OR *", true).as_deref(), Some("\"a\"\"b\" \"OR\" \"*\""), "the core quotes every word, FTS5 syntax stays out of reach");
    assert_eq!(fts_expression("   ", false), None, "an empty text adds no search condition");
}
