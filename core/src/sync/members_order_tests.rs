use serde_json::{json, Value};

use super::rig::*;
use super::{OperationDef, SyncConfig};
use crate::db::Db;
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape};
use crate::transport::{MockTransport, Reply};
use crate::value::ScalarKind;

fn users() -> ModelSpec {
    let mut spec = ModelSpec::new("users", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    let mut members = RelationSpec::new("chat_members", &[]);
    members.text_params = vec!["chatId".to_string(), "order".to_string()];
    members.text_types.insert("order".to_string(), "ChatMemberOrder".to_string());
    let mut remote = remote("ChatMembers", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes");
    remote.root = Some("node.members".to_string());
    remote.page_size = Some(30);
    remote.total_from = Some("node.membersCount".to_string());
    members.remote = Some(remote);
    spec.relations.push(members);
    spec
}

fn rig_with_db(db: Db) -> Rig {
    let rig = rig_on(db, MockTransport::new(), SyncConfig::default());
    let document = "query chatMembers($chatId: ID!, $order: ChatMemberOrder = JOINED, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { membersCount members(first: $first, after: $after, order: $order) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }";
    rig.sync.register(OperationDef::from_document("ChatMembers", document).unwrap());
    rig
}

fn members_rig() -> Rig {
    rig_with_db(Db::open_in_memory(vec![users()], None).unwrap())
}

fn scope(chat: &str, order: &str) -> serde_json::Map<String, Value> {
    params(json!({"chatId": chat, "order": order}))
}

fn page(ids: &[&str], has_next: bool, count: i64) -> Reply {
    let edges: Vec<Value> = ids.iter().map(|id| json!({"cursor": format!("{id}-cursor"), "node": {"id": id, "name": "x"}})).collect();
    Reply::data(json!({"node": {"membersCount": count, "members": {"edges": edges, "pageInfo": {"hasNextPage": has_next, "endCursor": format!("{}-cursor", ids.last().unwrap())}}}}))
}

fn total_of(rig: &Rig, params: &serde_json::Map<String, Value>) -> Option<i64> {
    let state = rig.sync.relation_state("users", "chat_members", params).unwrap();
    let total = state.borrow().total;
    total
}

#[tokio::test(start_paused = true)]
async fn owner_first_keeps_the_server_order_across_load_more_and_a_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("members.sqlite");
    let owner_first = scope("c1", "OWNER_FIRST");
    let server_order = ["z-owner", "a1", "b2", "nameless", "id9"];
    {
        let rig = rig_with_db(Db::open(&path, vec![users()], None).unwrap());
        rig.transport.enqueue("ChatMembers", page(&server_order[..3], true, 5));
        rig.transport.enqueue("ChatMembers", page(&server_order[3..], false, 5));
        rig.sync.load("users", "chat_members", &owner_first, Default::default()).await.unwrap();
        rig.sync.load_more("users", "chat_members", &owner_first).await.unwrap();
        assert_eq!(rig.transport.calls()[0].variables, json!({"chatId": "c1", "order": "OWNER_FIRST", "first": 30}));
        assert_eq!(rig.transport.calls()[1].variables, json!({"chatId": "c1", "order": "OWNER_FIRST", "first": 30, "after": "b2-cursor"}));
        assert_eq!(member_ids(&rig, "users", "chat_members", &owner_first), server_order);
    }
    let reopened = rig_with_db(Db::open(&path, vec![users()], None).unwrap());
    assert_eq!(member_ids(&reopened, "users", "chat_members", &owner_first), server_order);
}

#[tokio::test(start_paused = true)]
async fn each_order_of_one_chat_has_its_own_window_members_cursor_and_total() {
    let rig = members_rig();
    let (joined, owner_first) = (scope("c1", "JOINED"), scope("c1", "OWNER_FIRST"));
    rig.transport.enqueue("ChatMembers", page(&["j1", "j2"], true, 4));
    rig.transport.enqueue("ChatMembers", page(&["o1", "o2"], true, 4));
    rig.transport.enqueue("ChatMembers", page(&["o3", "o4"], false, 4));
    rig.transport.enqueue("ChatMembers", page(&["j3", "j4"], false, 4));
    rig.sync.load("users", "chat_members", &joined, Default::default()).await.unwrap();
    rig.sync.load("users", "chat_members", &owner_first, Default::default()).await.unwrap();
    rig.sync.load_more("users", "chat_members", &owner_first).await.unwrap();
    rig.sync.load_more("users", "chat_members", &joined).await.unwrap();
    let calls = rig.transport.calls();
    assert_eq!(calls[2].variables, json!({"chatId": "c1", "order": "OWNER_FIRST", "first": 30, "after": "o2-cursor"}));
    assert_eq!(calls[3].variables, json!({"chatId": "c1", "order": "JOINED", "first": 30, "after": "j2-cursor"}));
    assert_eq!(member_ids(&rig, "users", "chat_members", &joined), ["j1", "j2", "j3", "j4"]);
    assert_eq!(member_ids(&rig, "users", "chat_members", &owner_first), ["o1", "o2", "o3", "o4"]);
    assert_eq!((total_of(&rig, &joined), total_of(&rig, &owner_first)), (Some(4), Some(4)));
}

#[tokio::test(start_paused = true)]
async fn two_chats_stay_apart_in_the_same_order() {
    let rig = members_rig();
    let (one, two) = (scope("c1", "OWNER_FIRST"), scope("c2", "OWNER_FIRST"));
    rig.transport.enqueue("ChatMembers", page(&["a1", "a2"], false, 2));
    rig.transport.enqueue("ChatMembers", page(&["b1"], false, 1));
    rig.sync.load("users", "chat_members", &one, Default::default()).await.unwrap();
    rig.sync.load("users", "chat_members", &two, Default::default()).await.unwrap();
    assert_eq!(member_ids(&rig, "users", "chat_members", &one), ["a1", "a2"]);
    assert_eq!(member_ids(&rig, "users", "chat_members", &two), ["b1"]);
    assert_eq!((total_of(&rig, &one), total_of(&rig, &two)), (Some(2), Some(1)));
}
