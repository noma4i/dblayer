//! Cells of `data.md` for the decoding of nested values that come from the server: one function for every path, a bad row lands nothing.

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::schema::{FieldSpec, ModelSpec};
use crate::store::{Store, StoreError};
use crate::value::ScalarKind;

fn typed(mut field: FieldSpec, name: &str, members: Vec<FieldSpec>) -> FieldSpec {
    field.app_type = Some(name.into());
    field.fields = members;
    field
}

fn readings() -> Store {
    let members = vec![
        FieldSpec::scalar("name", ScalarKind::Str).nullable(),
        FieldSpec::scalar("tier", ScalarKind::Int).nullable().with_default(json!(1)),
        FieldSpec::scalar("rank", ScalarKind::Int).with_default(json!(2)),
        FieldSpec::scalar("code", ScalarKind::Str),
        FieldSpec::scalar("ref", ScalarKind::Id).nullable(),
        FieldSpec::scalar("flag", ScalarKind::Bool).nullable(),
        FieldSpec::scalar("ratio", ScalarKind::Num).nullable(),
        FieldSpec::scalar("when", ScalarKind::Date).nullable(),
        FieldSpec::enumeration("mood", &["calm", "loud"]).nullable(),
    ];
    let spec = ModelSpec::new("readings", vec![FieldSpec::scalar("id", ScalarKind::Id), typed(FieldSpec::object("box"), "Box", members).nullable()]);
    Store::open_models(None, vec![spec], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap()
}

fn land(db: &Store, row: Value) -> Result<(), StoreError> {
    let values = RowValue { row_id: "r1".into(), values: row.to_string() };
    let batch = StoreBatch { author: BatchAuthor::Outbox, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::Rows { model: "readings".into(), rows: vec![values] }] };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).map(|_| ())
}

fn stored(db: &Store) -> Option<Value> {
    db.read(&["readings"], |reader| reader.point("readings", "r1")).unwrap().map(Value::Object)
}

fn boxed(members: Value) -> Value {
    let mut full = json!({"code": "c"});
    full.as_object_mut().unwrap().extend(members.as_object().unwrap().clone());
    json!({"id": "r1", "box": full})
}

/// P-DATA-63: every path decodes a nested value by the same table: the stored value or an error of the row.
#[test]
fn data_nested_decode_table() {
    let ok: &[(&str, Value, (&str, Value))] = &[
        ("an unknown key is ignored", json!({"extra": 1}), ("code", json!("c"))),
        ("an absent nullable is null", json!({}), ("name", Value::Null)),
        ("an absent field with a default is the default", json!({}), ("rank", json!(2))),
        ("an absent nullable with a default is the default", json!({}), ("tier", json!(1))),
        ("a null nullable is null at any default", json!({"tier": null}), ("tier", Value::Null)),
        ("a null required with a default is the default", json!({"rank": null}), ("rank", json!(2))),
        ("an id of text", json!({"ref": "a1"}), ("ref", json!("a1"))),
        ("an id of an integer is text", json!({"ref": 42}), ("ref", json!("42"))),
        ("a bool", json!({"flag": true}), ("flag", json!(true))),
        ("a number with a fraction", json!({"ratio": 1.5}), ("ratio", json!(1.5))),
        ("a date is text", json!({"when": "2026-11-07T10:00:00Z"}), ("when", json!("2026-11-07T10:00:00Z"))),
        ("a declared enum value", json!({"mood": "loud"}), ("mood", json!("loud"))),
    ];
    for (case, members, (field, want)) in ok {
        let db = readings();
        land(&db, boxed(members.clone())).unwrap_or_else(|error| panic!("{case}: {error}"));
        let row = stored(&db).unwrap_or_else(|| panic!("{case}: no row"));
        assert_eq!(row["box"].get(*field).cloned().unwrap_or(Value::Null), *want, "{case}");
    }
    let bad: &[(&str, Value)] = &[
        ("a required without a default is absent", json!({"id": "r1", "box": {}})),
        ("a required without a default is null", boxed(json!({"code": null}))),
        ("a wrong type", boxed(json!({"tier": "one"}))),
        ("an id that is a bool", boxed(json!({"ref": true}))),
        ("an id that is a fraction", boxed(json!({"ref": 1.5}))),
        ("an id that is an object", boxed(json!({"ref": {"a": 1}}))),
        ("an id outside i64", boxed(json!({"ref": 18446744073709551615u64}))),
        ("a bool that is text", boxed(json!({"flag": "true"}))),
    ];
    for (case, row) in bad {
        let db = readings();
        assert!(matches!(land(&db, row.clone()), Err(StoreError::Invalid(_))), "{case}: the row landed");
    }
}

/// M71 (D-200): a required nested object that is null, or whose required member object is absent or null, refuses the row.
#[test]
fn data_required_nested_object_absent_or_null_refuses_row() {
    let inner = typed(FieldSpec::object("inner"), "Inner", vec![FieldSpec::scalar("v", ScalarKind::Str)]);
    let spec = ModelSpec::new("readings", vec![FieldSpec::scalar("id", ScalarKind::Id), typed(FieldSpec::object("box"), "Box", vec![inner])]);
    let db = Store::open_models(None, vec![spec], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    for (case, row) in [
        ("member absent", json!({"id": "r1", "box": {}})),
        ("member null", json!({"id": "r1", "box": {"inner": null}})),
        ("row attribute null", json!({"id": "r1", "box": null})),
    ] {
        assert!(matches!(land(&db, row), Err(StoreError::Invalid(_))), "{case}: the row landed");
    }
    assert!(stored(&db).is_none(), "no refused row is stored");
    land(&db, json!({"id": "r1", "box": {"inner": {"v": "x"}}})).unwrap();
    assert!(stored(&db).is_some(), "the complete row lands");
}

/// P-DATA-64: a row that does not decode lands nothing: neither the row nor a revision.
#[test]
fn data_nested_decode_error_lands_nothing() {
    let db = readings();
    let before = db.revision_seq().unwrap();
    assert!(matches!(land(&db, boxed(json!({"tier": "one"}))), Err(StoreError::Invalid(_))));
    assert!(stored(&db).is_none(), "the row is not stored");
    assert_eq!(db.revision_seq().unwrap(), before, "the revision did not move");
    land(&db, boxed(json!({"tier": 5}))).unwrap();
    assert!(stored(&db).is_some(), "the next good row lands");
}

/// P-DATA-68: a row of another layout is not read in part: the fingerprint of the model moves and the cache is rebuilt.
#[test]
fn data_nested_layout_mismatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let first = ModelSpec::new("readings", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("size", ScalarKind::Int).nullable()]);
    let second = ModelSpec::new("readings", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("size", ScalarKind::Str).nullable()]);
    {
        let db = Store::open_models(Some(&path), vec![first], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        crate::store::fixtures::put(&db, "readings", json!({"id": "r1", "size": 7}));
        assert!(stored(&db).is_some());
    }
    let db = Store::open_models(Some(&path), vec![second], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    assert!(stored(&db).is_none(), "the cache of the changed model is empty, no row is read in part");
}
