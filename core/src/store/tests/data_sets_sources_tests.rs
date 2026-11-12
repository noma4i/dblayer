use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreOp};
use crate::store::Store;

use super::data_sets_tests::*;
use super::sets_reference_tests::{stored_sets_of, Sets};

fn parent(id: &str, title: &str) -> RowValue {
    let values = json!({"id": id, "title": title, "lastMessageId": "m9", "lastMessageAt": 99, "lastSequenceNumber": 9, "lastMessageBody": "x"});
    RowValue { row_id: id.into(), values: values.to_string() }
}

fn land(db: &Arc<Store>, source: &str, row: RowValue) {
    match source {
        "page" | "node" | "event" => { deliver(db, 3, source, "chats", row, Vec::new()); }
        "answer" => write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![row] }]),
        "local create" => local_parent(db, "rows", serde_json::from_str::<Value>(&row.values).unwrap()),
        "local update" => parent_operation(db, 7, "update", &row),
        other => panic!("no source {other}"),
    }
}

/// P-DATA-56: a row of the parent from any source writes its other fields and never a `sets:` field: they stay the fields of the newest child, and a new parent has none.
#[test]
fn data_sets_parent_row_never_writes_them() {
    for source in ["page", "node", "event", "answer", "local create", "local update"] {
        let db = open();
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m5", 5)] }]);
        land(&db, source, parent("c1", "renamed"));
        let row = db.read(&["chats"], |reader| reader.point("chats", "c1")).unwrap().unwrap();
        assert_eq!(row.get("title"), Some(&json!("renamed")), "{source}: the other fields are written");
        assert_eq!(stored_sets_of(&db, "c1"), Sets { id: Some("m5".into()), at: Some(5), seq: None, body: Some("m5".into()) }, "{source}: a parent with a child");
        if source != "local update" {
            land(&db, source, parent("c2", "new"));
            assert_eq!(stored_sets_of(&db, "c2"), Sets::default(), "{source}: a parent without a child");
        }
    }
}
