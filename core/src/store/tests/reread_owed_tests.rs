use std::sync::Arc;

use serde_json::json;

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreOp};
use crate::schema::{FieldSpec, ModelSpec, NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
use crate::store::fixtures::put;
use crate::store::Store;
use crate::value::ScalarKind;

use super::data_sets_tests::{deliver, write_as};

fn chat_spec() -> ModelSpec {
    let mut chat = ModelSpec::new("Chat", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str)]);
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.into()];
    node.node = Some(NodeRead::One);
    let mut remote = crate::schema_fixtures::remote("ChatNode", RemoteShape::Single, "5.minutes");
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    chat.relations.push(node);
    chat
}

fn open() -> Arc<Store> {
    let db = Arc::new(Store::open_models(None, vec![chat_spec()], None, Arc::new(crate::runtime::SystemClock)).unwrap());
    put(&db, "Chat", json!({"id": "c1", "title": "t"}));
    db
}

fn owed(db: &Store, id: &str) -> bool {
    db.read(&["Chat"], |reader| super::super::reread_owed::is_owed(reader.connection(), "Chat", id)).unwrap()
}

fn owe(db: &Arc<Store>, batch: u64, id: &str) -> crate::store::batch_write::BatchWritten {
    write_as(db, batch, BatchAuthor::Feed, Vec::new(), vec![StoreOp::OweReread { model: "Chat".into(), id: id.into() }])
}

fn node_key(id: &str) -> WindowKey {
    WindowKey::relation("Chat", "node", &[(NODE_PARAM.to_string(), json!(id))].into_iter().collect())
}

fn land_node(db: &Arc<Store>, batch: u64, id: &str, roots: Vec<RowValue>) {
    let key = node_key(id);
    let op = StoreOp::Window { source: crate::schema::RowSource::NodeReread, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots, related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
    write_as(db, batch, BatchAuthor::Window, Vec::new(), vec![op]);
}

fn saved_stale(db: &Store, id: &str) -> bool {
    db.window_saved("Chat", "node", &[(NODE_PARAM.to_string(), json!(id))].into_iter().collect()).unwrap().expect("the node window is saved").stale
}

/// store_owe_reread_written: the batch of the event writes the debt of a stored row and announces the row of its table; a row that is not stored gets no debt and no revision.
#[test]
fn store_owe_reread_written() {
    let db = open();
    let before = db.revision_seq().unwrap();
    let written = owe(&db, 1, "c1");
    assert!(owed(&db, "c1"));
    assert!(written.notice.rows.as_ref().is_some_and(|rows| rows.iter().any(|(_, id)| id == "c1")), "the row is announced: {:?}", written.notice.rows);
    assert_eq!(written.revision, u64::try_from(before + 1).unwrap());
    let missing = owe(&db, 2, "c9");
    assert!(!owed(&db, "c9"));
    assert_eq!(missing.revision, written.revision, "a row that is not stored writes nothing");
}

/// store_owe_reread_same: the debt that stands is not written again, the batch announces nothing and keeps the revision.
#[test]
fn store_owe_reread_same() {
    let db = open();
    let first = owe(&db, 1, "c1");
    let again = owe(&db, 2, "c1");
    assert_eq!(again.revision, first.revision);
    assert!(again.notice.rows.as_ref().is_none_or(|rows| rows.is_empty()), "no row is announced: {:?}", again.notice.rows);
}

/// store_owed_cleared_by_server_row: every server source of the row pays the debt, the local write does not.
#[test]
fn store_owed_cleared_by_server_row() {
    let row = || RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "server"}).to_string() };
    for (path, pays) in [("event", true), ("page", true), ("node", true), ("response", true), ("local", false)] {
        let db = open();
        owe(&db, 1, "c1");
        if path == "local" {
            write_as(&db, 2, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::LocalRows { model: "Chat".into(), rows: vec![row()] }]);
        } else {
            deliver(&db, 2, path, "Chat", row(), Vec::new());
        }
        assert_eq!(owed(&db, "c1"), !pays, "{path}");
    }
}

/// store_owed_cleared_by_node_landing: the landing of the node window pays the debt although it writes no row.
#[test]
fn store_owed_cleared_by_node_landing() {
    let db = open();
    owe(&db, 1, "c1");
    land_node(&db, 2, "c1", Vec::new());
    assert!(!owed(&db, "c1"), "a null answer of the node pays the debt");
}

/// store_read_window_node_owed_is_stale: the saved node window of an owed row reads as stale, and the read of the view names the row.
#[test]
fn store_read_window_node_owed_is_stale() {
    let db = open();
    land_node(&db, 1, "c1", vec![RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "t"}).to_string() }]);
    assert!(!saved_stale(&db, "c1"));
    owe(&db, 2, "c1");
    assert!(saved_stale(&db, "c1"));
    let owing = db.read(&["Chat"], |reader| super::super::reread_owed::owed_among(reader.connection(), "Chat", &["c1".to_string(), "c2".to_string()])).unwrap();
    assert_eq!(owing.into_iter().collect::<Vec<_>>(), ["c1".to_string()]);
    land_node(&db, 3, "c1", vec![RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "again"}).to_string() }]);
    assert!(!saved_stale(&db, "c1"));
}
