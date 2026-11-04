use serde_json::{json, Value};

use super::rig::*;
use super::{OperationDef, SyncConfig};
use crate::db::Db;
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape};
use crate::transport::{MockTransport, Reply};
use crate::value::ScalarKind;

fn users(total_from: Option<&str>) -> ModelSpec {
    let mut spec = ModelSpec::new("users", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    let mut members = RelationSpec::new("chat_members", &[]);
    members.text_params = vec!["chatId".to_string()];
    let mut remote = remote("ChatMembers", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes");
    remote.root = Some("node.members".to_string());
    remote.page_size = Some(30);
    remote.total_from = total_from.map(str::to_string);
    members.remote = Some(remote);
    spec.relations.push(members);
    spec
}

fn rig_with_db(db: Db) -> Rig {
    let rig = rig_on(db, MockTransport::new(), SyncConfig::default());
    let document = "query chatMembers($chatId: ID!, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { membersCount members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }";
    rig.sync.register(OperationDef::from_document("ChatMembers", document).unwrap());
    rig
}

fn declared_rig() -> Rig {
    rig_with_db(Db::open_in_memory(vec![users(Some("node.membersCount"))], None).unwrap())
}

fn page(range: std::ops::Range<u32>, has_next: bool, count: Option<i64>, connection_total: Option<i64>) -> Reply {
    let edges: Vec<Value> = range.clone().map(|n| json!({"cursor": format!("u{n}"), "node": {"id": format!("u{n}"), "name": "x"}})).collect();
    let mut members = json!({"edges": edges, "pageInfo": {"hasNextPage": has_next, "endCursor": format!("u{}", range.end - 1)}});
    if let Some(total) = connection_total {
        members["totalCount"] = json!(total);
    }
    let mut node = json!({"members": members});
    if let Some(count) = count {
        node["membersCount"] = json!(count);
    }
    Reply::data(json!({"node": node}))
}

async fn total_of(rig: &Rig, chat: &str) -> Option<i64> {
    let state = rig.sync.relation_state("users", "chat_members", &params(json!({"chatId": chat}))).await.unwrap();
    let total = state.borrow().total;
    total
}

#[tokio::test(start_paused = true)]
async fn the_chat_member_count_becomes_the_total_of_a_first_page_of_30_rows() {
    let rig = declared_rig();
    let chat = params(json!({"chatId": "c1"}));
    rig.transport.enqueue("ChatMembers", page(0..30, true, Some(53), None));
    let landed = rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat, Default::default()).await.unwrap();
    assert_eq!(landed.rows, 30);
    assert_eq!(total_of(&rig, "c1").await, Some(53));
}

#[tokio::test(start_paused = true)]
async fn load_more_keeps_the_total_and_takes_the_newest_answer() {
    let rig = declared_rig();
    let chat = params(json!({"chatId": "c1"}));
    rig.transport.enqueue("ChatMembers", page(0..30, true, Some(53), None));
    rig.transport.enqueue("ChatMembers", page(30..45, true, Some(54), None));
    rig.transport.enqueue("ChatMembers", page(45..53, false, None, None));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat, Default::default()).await.unwrap();
    rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat).await.unwrap();
    assert_eq!(total_of(&rig, "c1").await, Some(54));
    rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &chat).await.unwrap();
    assert_eq!(total_of(&rig, "c1").await, Some(54), "an answer without the field keeps the last total");
}

#[tokio::test(start_paused = true)]
async fn each_chat_has_its_own_total() {
    let rig = declared_rig();
    rig.transport.enqueue("ChatMembers", page(0..3, true, Some(53), None));
    rig.transport.enqueue("ChatMembers", page(0..2, false, Some(2), None));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &params(json!({"chatId": "c1"})), Default::default()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &params(json!({"chatId": "c2"})), Default::default()).await.unwrap();
    assert_eq!((total_of(&rig, "c1").await, total_of(&rig, "c2").await), (Some(53), Some(2)));
}

#[tokio::test(start_paused = true)]
async fn the_total_survives_a_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("members.sqlite");
    {
        let rig = rig_with_db(Db::open(&path, vec![users(Some("node.membersCount"))], None).unwrap());
        rig.transport.enqueue("ChatMembers", page(0..30, true, Some(53), None));
        rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &params(json!({"chatId": "c1"})), Default::default()).await.unwrap();
    }
    let reopened = rig_with_db(Db::open(&path, vec![users(Some("node.membersCount"))], None).unwrap());
    assert_eq!(total_of(&reopened, "c1").await, Some(53));
}

#[tokio::test(start_paused = true)]
async fn without_a_declaration_the_total_is_the_total_count_of_the_connection() {
    let rig = rig_with_db(Db::open_in_memory(vec![users(None)], None).unwrap());
    rig.transport.enqueue("ChatMembers", page(0..3, true, Some(53), None));
    rig.transport.enqueue("ChatMembers", page(0..3, true, Some(53), Some(40)));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &params(json!({"chatId": "c1"})), Default::default()).await.unwrap();
    assert_eq!(total_of(&rig, "c1").await, None);
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "chat_members", &params(json!({"chatId": "c2"})), Default::default()).await.unwrap();
    assert_eq!(total_of(&rig, "c2").await, Some(40));
}
