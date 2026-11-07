use super::*;
use crate::store::Record;
use crate::identity::RowId;
use crate::schema::screens_fixture::screens_spec;
use crate::value::sync::SyncInfo;
use serde_json::{json, Value};
use std::sync::Arc;

fn text(value: &str) -> Cell {
    Cell::Text { value: value.to_string() }
}

fn spec_cell(elements: Value) -> Cell {
    let data = json!({"id": "s1", "spec": {"root": "r", "elements": elements}});
    let record = Record { id: RowId::new("s1").unwrap(), revision: 1, data: data.as_object().unwrap().clone(), sync: SyncInfo::default() };
    WireModel::of(&screens_spec()).row(&Arc::new(record)).cells.remove(1)
}

#[test]
fn an_interface_element_cell_holds_the_typename_then_the_shared_members_then_the_variant_members() {
    let cell = spec_cell(json!([{"__typename": "SampleColumn", "id": "c1", "children": ["a"], "gap": 2}, {"__typename": "SampleFuture", "id": "f"}, {"id": "bad"}]));
    let column = Cell::Seq { items: vec![text("SampleColumn"), Cell::Seq { items: vec![text("c1"), Cell::Seq { items: vec![text("a")] }, Cell::Null, Cell::Real { value: 2.0 }] }] };
    let unknown = Cell::Seq { items: vec![text("SampleFuture"), Cell::Null] };
    assert_eq!(cell, Cell::Seq { items: vec![text("r"), Cell::Seq { items: vec![column, unknown] }] });
}

#[test]
fn the_layout_hash_follows_the_shared_fields() {
    let mut other = screens_spec();
    other.fields[2].fields[1].shared.pop();
    assert_ne!(wire_layout(&screens_spec()), wire_layout(&other));
}
