use super::*;
use crate::store::Record;
use crate::identity::RowId;
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::sync::SyncInfo;
use crate::value::ScalarKind;
use serde_json::{json, Value};
use std::sync::Arc;

fn spec(second: &str) -> ModelSpec {
    let variants = vec![
        FieldSpec::variant("gift", "GiftAttachment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("status", ScalarKind::Str).nullable()]),
        FieldSpec::variant("reaction", second, vec![FieldSpec::scalar("body", ScalarKind::Str)]),
    ];
    ModelSpec::new("wires", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::union("attachment", "WireAttachment", variants).nullable()])
}

fn cells(attachment: Value) -> Cell {
    let record = Record { id: RowId::new("w1").unwrap(), revision: 1, data: json!({"id": "w1", "attachment": attachment}).as_object().unwrap().clone(), sync: SyncInfo::default() };
    WireModel::of(&spec("ReactionAttachment")).row(&Arc::new(record)).cells.remove(0)
}

fn text(value: &str) -> Cell {
    Cell::Text { value: value.to_string() }
}

#[test]
fn a_union_cell_holds_the_typename_and_the_variant_members() {
    let known = cells(json!({"__typename": "GiftAttachment", "id": "g1"}));
    assert_eq!(known, Cell::Seq { items: vec![text("GiftAttachment"), Cell::Seq { items: vec![text("g1"), Cell::Null] }] });
    assert_eq!(cells(json!({"__typename": "FutureAttachment"})), Cell::Seq { items: vec![text("FutureAttachment"), Cell::Null] });
    assert_eq!(cells(Value::Null), Cell::Null);
}

#[test]
fn the_layout_hash_follows_the_variant_type_names() {
    assert_ne!(wire_layout(&spec("ReactionAttachment")), wire_layout(&spec("EmojiAttachment")));
}
