use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::schema::{FieldSpec, ModelSpec};
use crate::store::Store;
use crate::value::ScalarKind;

fn write(db: &Arc<Store>, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author, id: 1, epoch: 1, basis: Default::default(), rows, intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
}

fn doc(revision: i64, text: &str) -> RowValue {
    RowValue { row_id: "d1".into(), values: json!({"id": "d1", "rev": revision, "text": text}).to_string() }
}

fn text(db: &Store) -> Value {
    db.read(&["docs"], |reader| reader.point("docs", "d1")).unwrap().and_then(|row| row.get("text").cloned()).unwrap_or(Value::Null)
}

#[test]
fn data_server_revision_drops_older() {
    let spec = ModelSpec { server_revision: Some("rev".into()), ..ModelSpec::new("docs", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("rev", ScalarKind::Int).optional(), FieldSpec::scalar("text", ScalarKind::Str).optional()]) };
    let db = Arc::new(Store::open_models(None, vec![spec], None).unwrap());
    let rows = |row: RowValue| vec![StoreOp::Rows { model: "docs".into(), rows: vec![row] }];
    write(&db, BatchAuthor::Outbox, Vec::new(), rows(doc(5, "first")));
    assert_eq!(text(&db), json!("first"), "no stored revision: the first version lands");
    write(&db, BatchAuthor::Outbox, Vec::new(), rows(doc(3, "older answer")));
    assert_eq!(text(&db), json!("first"), "an answer below the stored revision is dropped");
    let event = |row: &RowValue| (String::from("docs"), json!({"op": "upsert", "id": "d1", "row": serde_json::from_str::<Value>(&row.values).unwrap()}).to_string());
    write(&db, BatchAuthor::Feed, vec![event(&doc(4, "older event"))], Vec::new());
    assert_eq!(text(&db), json!("first"), "an event below it is dropped");
    let key = WindowKey::nodes("docs", &["d1"]);
    let page = |row: RowValue, base: i64| vec![StoreOp::Window { key: key.name.clone(), model: key.model.clone(), page: 1, replace_all: false, cursor: None, roots: vec![row], related: Vec::new(), total: None, base_revision: base }];
    write(&db, BatchAuthor::Window, Vec::new(), page(doc(2, "older page"), db.revision_seq().unwrap()));
    assert_eq!(text(&db), json!("first"), "a page below it is dropped");
    write(&db, BatchAuthor::Outbox, Vec::new(), rows(doc(5, "equal")));
    assert_eq!(text(&db), json!("equal"), "an equal revision lands");
    write(&db, BatchAuthor::Outbox, Vec::new(), rows(doc(6, "newer")));
    assert_eq!(text(&db), json!("newer"), "a greater revision lands");
    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "docs".into(), rows: vec![doc(1, "optimistic")] }]);
    assert_eq!(text(&db), json!("optimistic"), "an optimistic write does not check the rule");
}
