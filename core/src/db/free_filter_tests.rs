use super::fixtures::*;
use super::*;
use crate::query::{Query, Where};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use serde_json::json;

fn blobs() -> ModelSpec {
    ModelSpec::new("blobs", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::raw("data").nullable().optional()])
}

fn ids(db: &Db, clause: Where) -> Vec<String> {
    let mut found: Vec<String> = db.query(&Query::new("blobs").filtered(clause)).unwrap().iter().map(|row| row.id.to_string()).collect();
    found.sort();
    found
}

#[test]
fn a_filter_on_a_free_field_sees_presence_and_scalar_values() {
    let db = Db::open_in_memory(vec![blobs()], None).unwrap();
    put(&db, "blobs", json!({"id": "b1", "data": {"a": 1}}));
    put(&db, "blobs", json!({"id": "b2", "data": 5}));
    put(&db, "blobs", json!({"id": "b3"}));
    assert_eq!(ids(&db, Where::IsNull { field: "data".into() }), ["b3"]);
    assert_eq!(ids(&db, Where::equal("data", json!(5))), ["b2"]);
    assert_eq!(ids(&db, Where::negated(Where::IsNull { field: "data".into() })), ["b1", "b2"]);
}
