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
fn equality_compiles_null_handling() {
    assert_eq!(compiled(&Where::equal("status", json!(null))), ("r.status IS NULL".to_string(), vec![]));
    assert_eq!(
        compiled(&Where::equal("status", json!("active"))),
        ("r.status = ?".to_string(), vec![SqlValue::Text("active".into())])
    );
    assert_eq!(compiled(&Where::not_equal("status", json!(null))), ("r.status IS NOT NULL".to_string(), vec![]));
    assert_eq!(
        compiled(&Where::not_equal("status", json!("active"))),
        ("r.status IS NOT ?".to_string(), vec![SqlValue::Text("active".into())])
    );
    assert_eq!(compiled(&Where::is_null("status")), ("r.status IS NULL".to_string(), vec![]));
}

#[test]
fn comparisons_compile_each_operator() {
    let cases = [
        (CompareOp::Less, "r.age < ?"),
        (CompareOp::LessOrEqual, "r.age <= ?"),
        (CompareOp::Greater, "r.age > ?"),
        (CompareOp::GreaterOrEqual, "r.age >= ?"),
    ];
    for (operator, expected) in cases {
        assert_eq!(compiled(&Where::compare("age", operator, json!(18))), (expected.to_string(), vec![SqlValue::Integer(18)]));
    }
}

#[test]
fn lists_guard_against_null_and_empty() {
    let values = vec![json!("a"), json!("b")];
    let texts = vec![SqlValue::Text("a".into()), SqlValue::Text("b".into())];
    assert_eq!(compiled(&Where::in_list("chatId", values.clone())), ("r.chat_id IN (?, ?)".to_string(), texts.clone()));
    assert_eq!(
        compiled(&Where::not_in_list("chatId", values)),
        ("(r.chat_id IS NULL OR r.chat_id NOT IN (?, ?))".to_string(), texts)
    );
    assert_eq!(compiled(&Where::in_list("chatId", vec![])), ("0".to_string(), vec![]));
    assert_eq!(compiled(&Where::not_in_list("chatId", vec![])), ("1".to_string(), vec![]));
}

#[test]
fn null_members_never_reach_the_list_binding() {
    let (sql, bindings) = compiled(&Where::in_list("age", vec![json!(1), json!(null)]));
    assert_eq!((sql.as_str(), bindings), ("r.age IN (?)", vec![SqlValue::Integer(1)]));
    assert_eq!(compiled(&Where::in_list("age", vec![json!(null)])), ("0".to_string(), vec![]));
    assert_eq!(compiled(&Where::not_in_list("age", vec![json!(null)])), ("1".to_string(), vec![]));
}

#[test]
fn negation_resolves_unknown_before_not() {
    let (sql, bindings) = compiled(&Where::negated(Where::equal("enabled", json!(true))));
    assert_eq!(sql, "NOT COALESCE(r.enabled = ?, 0)");
    assert_eq!(bindings, vec![SqlValue::Integer(1)]);
}

#[test]
fn conjunctions_wrap_parts_in_order() {
    let parts = vec![Where::equal("status", json!("on")), Where::compare("age", CompareOp::Greater, json!(18))];
    let expected = vec![SqlValue::Text("on".into()), SqlValue::Integer(18)];
    assert_eq!(compiled(&Where::all_of(parts.clone())), ("(r.status = ? AND r.age > ?)".to_string(), expected.clone()));
    assert_eq!(compiled(&Where::any_of(parts)), ("(r.status = ? OR r.age > ?)".to_string(), expected));
    assert_eq!(compiled(&Where::all_of(vec![])), ("1".to_string(), vec![]));
    assert_eq!(compiled(&Where::any_of(vec![])), ("0".to_string(), vec![]));
}

#[test]
fn unpromoted_fields_use_json_extract() {
    assert_eq!(
        compiled(&Where::equal("kind", json!("chat"))),
        ("json_extract(r.payload, '$.kind') = ?".to_string(), vec![SqlValue::Text("chat".into())])
    );
    let mixed = Where::all_of(vec![Where::equal("chatId", json!("c1")), Where::compare("kind", CompareOp::Greater, json!("a"))]);
    assert_eq!(compiled(&mixed).0, "(r.chat_id = ? AND json_extract(r.payload, '$.kind') > ?)");
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
