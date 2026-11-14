//! Cells of `data.md` for the layout of nested values in the Store; the decoding of a server row is tested through the page answer in `spec/data_server_decode_tests.rs`.

use serde_json::{json, Value};

use crate::schema::{FieldSpec, ModelSpec};
use crate::store::Store;
use crate::value::ScalarKind;

fn stored(db: &Store) -> Option<Value> {
    db.read(&["readings"], |reader| reader.point("readings", "r1")).unwrap().map(Value::Object)
}

/// P-DATA-68: a row of another layout is not read in part: the fingerprint of the model moves and the cache is rebuilt.
#[test]
fn data_nested_layout_mismatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let first = ModelSpec::new("readings", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("size", ScalarKind::Int).nullable()]);
    let second = ModelSpec::new("readings", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("size", ScalarKind::Str).nullable()]);
    {
        let db = Store::open_models(Some(&path), vec![first], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        crate::store::fixtures::put(&db, "readings", json!({"id": "r1", "size": 7}));
        assert!(stored(&db).is_some());
    }
    let db = Store::open_models(Some(&path), vec![second], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    assert!(stored(&db).is_none(), "the cache of the changed model is empty, no row is read in part");
}
