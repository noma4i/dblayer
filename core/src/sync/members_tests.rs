use serde_json::{json, Value};

use super::rig::*;
use super::OperationDef;
use crate::db::Db;
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape};
use crate::transport::{MockTransport, Reply};
use crate::value::ScalarKind;
use super::SyncConfig;

fn users() -> ModelSpec {
    let mut spec = ModelSpec::new("users", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    let mut members = RelationSpec::new("chat_members", &[]);
    members.text_params = vec!["chatId".to_string()];
    let mut remote = remote("ChatMembers", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes");
    remote.root = Some("node.members".to_string());
    remote.page_size = Some(30);
    members.remote = Some(remote);
    spec.relations.push(members);
    spec
}

fn members_rig() -> Rig {
    rig_with_db(Db::open_in_memory(vec![users()], None).unwrap())
}

fn rig_with_db(db: Db) -> Rig {
    let rig = rig_on(db, MockTransport::new(), SyncConfig::default());
    let document = "query chatMembers($chatId: ID!, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }";
    rig.sync.register(OperationDef::from_document("ChatMembers", document).unwrap());
    rig
}

fn page(range: std::ops::Range<u32>, prefix: &str, has_next: bool) -> Reply {
    let edges: Vec<Value> = range.clone().map(|n| json!({"cursor": format!("{prefix}{n}"), "node": {"id": format!("{prefix}{n}"), "name": "x"}})).collect();
    let end = range.end - 1;
    Reply::data(json!({"node": {"members": {"edges": edges, "pageInfo": {"hasNextPage": has_next, "endCursor": format!("{prefix}{end}")}}}}))
}

fn ids(prefix: &str, range: std::ops::Range<u32>) -> Vec<String> {
    range.map(|n| format!("{prefix}{n}")).collect()
}

#[tokio::test(start_paused = true)]
async fn load_more_reads_the_second_page_with_the_cursor_of_the_first() {
    let rig = members_rig();
    let chat = params(json!({"chatId": "c1"}));
    rig.transport.enqueue("ChatMembers", page(0..30, "u", true));
    rig.transport.enqueue("ChatMembers", page(30..45, "u", false));
    let first = rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat, Default::default()).await.unwrap();
    assert_eq!((first.rows, first.has_next), (30, true));
    let second = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat).await.unwrap();
    assert_eq!((second.rows, second.has_next), (15, false));
    assert_eq!(rig.transport.calls()[0].variables, json!({"chatId": "c1", "first": 30}));
    assert_eq!(rig.transport.calls()[1].variables, json!({"chatId": "c1", "first": 30, "after": "u29"}));
    assert_eq!(member_ids(&rig, "users", "chat_members", &chat), ids("u", 0..45));
}

#[tokio::test(start_paused = true)]
async fn a_second_load_inside_fresh_for_does_not_reach_the_network() {
    let rig = members_rig();
    let chat = params(json!({"chatId": "c1"}));
    rig.transport.enqueue("ChatMembers", page(0..2, "u", false));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat, Default::default()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat, Default::default()).await.unwrap();
    assert_eq!(rig.transport.call_count("ChatMembers"), 1);
}

#[tokio::test(start_paused = true)]
async fn two_chats_keep_separate_windows() {
    let rig = members_rig();
    let (one, two) = (params(json!({"chatId": "c1"})), params(json!({"chatId": "c2"})));
    rig.transport.enqueue("ChatMembers", page(0..3, "a", true));
    rig.transport.enqueue("ChatMembers", page(0..2, "b", false));
    rig.transport.enqueue("ChatMembers", page(3..5, "a", false));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &one, Default::default()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &two, Default::default()).await.unwrap();
    rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &one).await.unwrap();
    assert_eq!(rig.transport.calls()[2].variables, json!({"chatId": "c1", "first": 30, "after": "a2"}));
    assert_eq!(member_ids(&rig, "users", "chat_members", &one), ids("a", 0..5));
    assert_eq!(member_ids(&rig, "users", "chat_members", &two), ids("b", 0..2));
}

fn named_page(ids: &[&str], has_next: bool) -> Reply {
    let edges: Vec<Value> = ids.iter().map(|id| json!({"cursor": id, "node": {"id": id, "name": "x"}})).collect();
    Reply::data(json!({"node": {"members": {"edges": edges, "pageInfo": {"hasNextPage": has_next, "endCursor": ids.last()}}}}))
}

#[tokio::test(start_paused = true)]
async fn the_server_order_survives_load_more_a_refresh_by_another_list_and_a_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("members.sqlite");
    let chat = params(json!({"chatId": "c1"}));
    let server_order = ["u5", "u1", "u9", "u3", "u0"];
    {
        let rig = rig_with_db(Db::open(&path, vec![users()], None).unwrap());
        rig.transport.enqueue("ChatMembers", named_page(&server_order[..3], true));
        rig.transport.enqueue("ChatMembers", named_page(&server_order[3..], false));
        rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat, Default::default()).await.unwrap();
        assert_eq!(member_ids(&rig, "users", "chat_members", &chat), server_order[..3]);
        rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat).await.unwrap();
        put(&rig, "users", json!({"id": "u1", "name": "renamed"}));
        put(&rig, "users", json!({"id": "u0", "name": "a"}));
        assert_eq!(member_ids(&rig, "users", "chat_members", &chat), server_order);
        let state = rig.sync.relation_state("users", "chat_members", &chat).await.unwrap();
        assert_eq!(state.borrow().total, None);
    }
    let reopened = rig_with_db(Db::open(&path, vec![users()], None).unwrap());
    assert_eq!(member_ids(&reopened, "users", "chat_members", &chat), server_order);
}
