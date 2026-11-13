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
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    let mut remote = crate::schema_fixtures::remote("ChatList", RemoteShape::List { coverage: crate::schema::Coverage::Complete }, "5.minutes");
    remote.variables.insert("list".into(), VariableSource::Param { name: "list".into() });
    list.remote = Some(remote);
    chat.relations.extend([node, list]);
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
    let base = db.revision_seq().unwrap();
    land_node_at(db, batch, id, roots, base);
}

fn land_node_at(db: &Arc<Store>, batch: u64, id: &str, roots: Vec<RowValue>, base_revision: i64) {
    let key = node_key(id);
    let op = StoreOp::Window { source: crate::schema::RowSource::NodeReread, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots, related: Vec::new(), total: None, base_revision };
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
    assert!(again.revision >= first.revision);
    assert!(again.notice.rows.as_ref().is_none_or(|rows| rows.is_empty()), "no row is announced: {:?}", again.notice.rows);
}

/// store_owed_cleared_by_server_row: every server source of the row pays the debt, the local write does not.
#[test]
fn store_owed_cleared_by_server_row() {
    let row = || RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "server"}).to_string() };
    for (path, pays) in [("event", true), ("page", true), ("node", true), ("response", false), ("local", false)] {
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
    let query = crate::store::query::Query::new("Chat");
    let (_, _, _, read, _) = db.read_view(&query, false, 0, 0, None, None, &crate::store::fixtures::list_view(&query), (0, 0)).unwrap();
    assert!(matches!(read, crate::store::lookup::ViewRead::Rows(_, _, ref owed) if owed.iter().eq(["c1".to_string()].iter())), "the value of the reader names the row in debt");
    land_node(&db, 3, "c1", vec![RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "again"}).to_string() }]);
    assert!(!saved_stale(&db, "c1"));
}

/// feed_event_debt_survives_restart: the debt of the event of every form is in the file, a store opened again reads it.
#[test]
fn feed_event_debt_survives_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let list = WindowKey::relation("Chat", "list", &[("list".to_string(), json!("PRIMARY"))].into_iter().collect());
    let params: serde_json::Map<String, serde_json::Value> = [("list".to_string(), json!("PRIMARY"))].into_iter().collect();
    {
        let db = Arc::new(Store::open_models(Some(path.as_path()), vec![chat_spec()], None, Arc::new(crate::runtime::SystemClock)).unwrap());
        put(&db, "Chat", json!({"id": "c1", "title": "t"}));
        let op = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: list.name.clone(), model: list.model.clone(), page: 1, replace_all: false, cursor: None, roots: Vec::new(), related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
        write_as(&db, 1, BatchAuthor::Window, Vec::new(), vec![op]);
        assert!(!db.window_saved("Chat", "list", &params).unwrap().expect("the list is saved").stale);
        write_as(&db, 2, BatchAuthor::Feed, Vec::new(), vec![StoreOp::OweReread { model: "Chat".into(), id: "c1".into() }, StoreOp::StaleWindows { keys: vec![("Chat".into(), list.name.clone())] }]);
    }
    let db = Arc::new(Store::open_models(Some(path.as_path()), vec![chat_spec()], None, Arc::new(crate::runtime::SystemClock)).unwrap());
    assert!(owed(&db, "c1"), "the row still owes the reread");
    assert!(db.window_saved("Chat", "list", &params).unwrap().expect("the list is saved").stale, "the saved list still owes the reread");
}

/// store_owed_kept_by_an_answer_older_than_the_event: a load issued before the event does not pay the debt of the event, the next load does, for the node and for the page of a list; a write without the request of a load pays nothing.
#[test]
fn store_owed_kept_by_an_answer_older_than_the_event() {
    let row = || RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "answer"}).to_string() };
    let page = |db: &Arc<Store>, batch: u64, base_revision: i64| {
        let key = WindowKey::nodes("Chat", &["c1"]);
        let op = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![row()], related: Vec::new(), total: None, base_revision };
        write_as(db, batch, BatchAuthor::Window, Vec::new(), vec![op]);
    };
    let node = |db: &Arc<Store>, batch: u64, base_revision: i64| land_node_at(db, batch, "c1", vec![row()], base_revision);
    type Land = Box<dyn Fn(&Arc<Store>, u64, i64)>;
    let paths: [(&str, Land); 2] = [("node", Box::new(node)), ("page", Box::new(page))];
    for (path, land) in paths {
        let db = open();
        let issued = db.revision_seq().unwrap();
        owe(&db, 1, "c1");
        land(&db, 2, issued);
        assert!(owed(&db, "c1"), "{path}: the answer is older than the event");
        if path == "node" {
            land_node_at(&db, 3, "c1", Vec::new(), issued);
            assert!(owed(&db, "c1"), "a null answer of an older request pays nothing");
        }
        land(&db, 4, db.revision_seq().unwrap());
        assert!(!owed(&db, "c1"), "{path}: the next load pays");
    }
    let local = |db: &Arc<Store>| write_as(db, 2, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::LocalRows { model: "Chat".into(), rows: vec![row()] }]);
    let fields = |db: &Arc<Store>| write_as(db, 2, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::LocalFields { model: "Chat".into(), row_id: "c1".into(), values: json!({"title": "local"}).to_string() }]);
    let response = |db: &Arc<Store>| write_as(db, 2, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "Chat".into(), rows: vec![row()] }]);
    type Write = Box<dyn Fn(&Arc<Store>) -> crate::store::batch_write::BatchWritten>;
    let writes: [(&str, Write); 3] = [("local rows", Box::new(local)), ("local fields", Box::new(fields)), ("response", Box::new(response))];
    for (path, write) in writes {
        let db = open();
        owe(&db, 1, "c1");
        write(&db);
        assert!(owed(&db, "c1"), "{path}: a write without the request of a load keeps the debt");
    }
}

/// store_owed_goes_with_the_row: the delete of the row and the end of the account take the debt with them, a row created again under the same id is not in debt.
#[test]
fn store_owed_goes_with_the_row() {
    let row = || RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "again"}).to_string() };
    let removed = |db: &Arc<Store>| { write_as(db, 2, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveServerRows { model: "Chat".into(), ids: vec!["c1".into()] }]); };
    let account = |erase_encrypted: bool| move |db: &Arc<Store>| { db.erase_account(db.generation(), erase_encrypted).unwrap(); };
    type Ends = Box<dyn Fn(&Arc<Store>)>;
    let ends: [(&str, Ends); 3] = [("delete", Box::new(removed)), ("account", Box::new(account(false))), ("session end", Box::new(account(true)))];
    for (path, end) in ends {
        let db = open();
        owe(&db, 1, "c1");
        assert!(owed(&db, "c1"), "{path}: the debt stands");
        end(&db);
        put(&db, "Chat", json!({"id": "c1", "title": "again"}));
        write_as(&db, 3, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::LocalRows { model: "Chat".into(), rows: vec![row()] }]);
        assert!(!owed(&db, "c1"), "{path}: the row created again owes nothing");
    }
}
