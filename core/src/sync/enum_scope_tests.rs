use serde_json::{json, Value};

use super::rig::*;
use super::OperationDef;
use crate::db::Db;
use crate::identity::RowId;
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape};
use crate::transport::{MockTransport, Reply};
use crate::value::ScalarKind;
use super::SyncConfig;

fn chats() -> ModelSpec {
    let lists = FieldSpec { app_type: Some("ChatList".to_string()), values: vec!["PRIMARY".to_string(), "SECONDARY".to_string()], ..FieldSpec::list("lists", ScalarKind::Str) };
    let mut spec = ModelSpec::new("chats", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str).optional(), lists.optional()]);
    let mut inbox = RelationSpec::new("list", &[]);
    inbox.text_params = vec!["list".to_string()];
    inbox.text_types.insert("list".to_string(), "ChatList".to_string());
    let mut remote = remote("Chats", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes");
    remote.root = Some("viewer.chats".to_string());
    remote.page_size = Some(25);
    inbox.remote = Some(remote);
    spec.relations.push(inbox);
    spec
}

fn chats_rig() -> Rig {
    let rig = rig_on(Db::open_in_memory(vec![chats()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    let document = "query Chats($list: ChatList, $first: Int, $after: Cursor) { viewer { chats(filter: { list: $list }, first: $first, after: $after) { edges { cursor node { id title lists } } pageInfo { hasNextPage endCursor } } } }";
    rig.sync.register(OperationDef::from_document("Chats", document).unwrap());
    rig
}

fn answer(chats: &[(&str, &[&str])]) -> Reply {
    let edges: Vec<Value> = chats.iter().map(|(id, lists)| json!({"cursor": id, "node": {"id": id, "title": id, "lists": lists}})).collect();
    Reply::data(json!({"viewer": {"chats": {"edges": edges, "pageInfo": {"hasNextPage": false, "endCursor": chats.last().map(|(id, _)| *id)}}}}))
}

#[tokio::test(start_paused = true)]
async fn each_list_value_has_its_window_and_the_server_answer_decides_the_members() {
    let rig = chats_rig();
    let (primary, secondary) = (params(json!({"list": "PRIMARY"})), params(json!({"list": "SECONDARY"})));
    rig.transport.enqueue("Chats", answer(&[("c1", &["PRIMARY", "SECONDARY"]), ("c2", &["PRIMARY"])]));
    rig.transport.enqueue("Chats", answer(&[("c1", &["PRIMARY", "SECONDARY"]), ("c3", &["SECONDARY", "FUTURE"])]));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "chats", "list", &primary, Default::default()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "chats", "list", &secondary, Default::default()).await.unwrap();
    assert_eq!(rig.transport.calls()[0].variables, json!({"list": "PRIMARY", "first": 25}));
    assert_eq!(rig.transport.calls()[1].variables, json!({"list": "SECONDARY", "first": 25}));
    assert_eq!(member_ids(&rig, "chats", "list", &primary), ["c1", "c2"]);
    assert_eq!(member_ids(&rig, "chats", "list", &secondary), ["c1", "c3"]);
    let stored = rig.db.find("chats", &RowId::new("c3").unwrap()).unwrap().unwrap();
    assert_eq!(stored.get("lists"), Some(&json!(["SECONDARY", "FUTURE"])));
}

#[tokio::test(start_paused = true)]
async fn a_stale_window_reloads_and_a_chat_the_server_dropped_leaves_only_that_window() {
    let rig = chats_rig();
    let (primary, secondary) = (params(json!({"list": "PRIMARY"})), params(json!({"list": "SECONDARY"})));
    rig.transport.enqueue("Chats", answer(&[("c1", &["PRIMARY", "SECONDARY"]), ("c2", &["PRIMARY"])]));
    rig.transport.enqueue("Chats", answer(&[("c1", &["PRIMARY", "SECONDARY"])]));
    rig.transport.enqueue("Chats", answer(&[("c2", &["PRIMARY"])]));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "chats", "list", &primary, Default::default()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "chats", "list", &secondary, Default::default()).await.unwrap();
    rig.db.shared.store.invalidate_family_now("chats", "list").unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "chats", "list", &primary, Default::default()).await.unwrap();
    assert_eq!(rig.transport.call_count("Chats"), 3);
    assert_eq!(member_ids(&rig, "chats", "list", &primary), ["c2"]);
    assert_eq!(member_ids(&rig, "chats", "list", &secondary), ["c1"]);
}
