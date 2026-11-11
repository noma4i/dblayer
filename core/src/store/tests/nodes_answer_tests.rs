//! P-WIN-26: `nodes(ids)` gives only the ids that came back as rows in the answer of its own request, in the order of the call.

use std::sync::Arc;

use serde_json::json;

use crate::identity::RowId;
use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

fn open() -> Arc<Store> {
    let db = Arc::new(Store::open_models(None, all_specs(), None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    for id in ["m1", "m2", "m3"] {
        put(&db, "moments", json!({"id": id, "userId": "u1", "text": format!("cached {id}")}));
    }
    db
}

fn answer(db: &Store, key: &WindowKey, returned: &[&str]) {
    let roots = returned.iter().map(|id| RowValue { row_id: (*id).into(), values: json!({"id": id, "userId": "u1", "text": format!("served {id}")}).to_string() }).collect();
    let page = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name.clone(), model: key.model.clone(), page: 1, replace_all: false, cursor: None, roots, related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
    let batch = StoreBatch { author: BatchAuthor::Window, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![page] };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
}

fn ids(names: &[&str]) -> Vec<RowId> { names.iter().map(|name| RowId::new(name).unwrap()).collect() }

fn answered(db: &Store, key: &WindowKey, asked: &[&str]) -> Vec<String> {
    db.nodes_answered(&key.model, &key.name, &ids(asked)).unwrap().iter().map(|id| id.as_str().to_string()).collect()
}

#[test]
fn win_prop_nodes_answer_only() {
    let asked = ["m3", "m1", "m2"];
    let key = WindowKey::nodes("moments", &asked);
    let db = open();
    answer(&db, &key, &[]);
    assert!(answered(&db, &key, &asked).is_empty(), "every id answered null: nothing comes back");
    answer(&db, &key, &["m1", "m3"]);
    assert_eq!(answered(&db, &key, &asked), ["m3", "m1"], "a mix of rows and null: the rows in the order of the call, without the null");
    let cached = db.read(&["moments"], |reader| reader.point("moments", "m2")).unwrap();
    assert!(cached.is_some_and(|row| row.get("text") == Some(&json!("cached m2"))), "the cached row of an id the server did not return stays in the database");
    answer(&db, &key, &["m2"]);
    assert_eq!(answered(&db, &key, &asked), ["m2"], "the next answer replaces the previous one");
}
