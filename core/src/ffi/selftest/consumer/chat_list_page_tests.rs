use std::sync::Arc;

use serde_json::{json, Value};

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tape::Rows;
use crate::ffi::{QuerySpec, ReadRequest};
use crate::test_wait::settled;

const REAL_PAGE: &str = include_str!("../../../../tests/corpus/consumer/chats_primary_user54.json");

fn list_query(list: &str) -> QuerySpec {
    QuerySpec::json(json!({"model": "Chat", "relation": "list", "params": {"list": list}}).to_string())
}

fn page_ids(page: &Value) -> Vec<String> {
    page["viewer"]["chats"]["edges"].as_array().unwrap().iter().map(|edge| edge["node"]["id"].as_str().unwrap().to_string()).collect()
}

fn sorted(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids
}

async fn local_ids(stand: &Stand, list: &str) -> Vec<String> {
    let epoch = stand.database.session_epoch();
    sorted(bound(stand.database.read_query(epoch, list_query(list))).await.unwrap().into_iter().map(|row| row.id).collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_chat_list_keeps_every_row_of_a_real_page() {
    let page: Value = serde_json::from_str::<Value>(REAL_PAGE).unwrap()["data"].clone();
    let want = page_ids(&page);
    assert_eq!(want.len(), 25);
    assert!(want.iter().any(|id| id == "djEuQ2hhdDozNjQxNjE="));
    let stand = Stand::prepare().await;
    stand.answer("chats", page);
    stand.sign_in().await;
    let rows = Arc::new(Rows::default());
    let _stream = stand.database.watch(ReadRequest::List { query: list_query("PRIMARY") }, rows.clone());
    settled(&stand, "the first page of the list", || rows.len() > 0).await;
    stand.settle().await;
    let have = local_ids(&stand, "PRIMARY").await;
    let missing: Vec<_> = sorted(want.clone()).into_iter().filter(|id| !have.contains(id)).collect();
    assert!(missing.is_empty(), "missing from Chat.list PRIMARY: {missing:?}, local {} of {}", have.len(), want.len());
    assert_eq!(rows.len(), want.len(), "the stream holds every row");
    stand.database.close_now();
}

fn real_node() -> Value {
    serde_json::from_str::<Value>(REAL_PAGE).unwrap()["data"]["viewer"]["chats"]["edges"][5]["node"].clone()
}

fn node_of(id: &str, list: &str) -> Value {
    let mut node = real_node();
    node["id"] = json!(id);
    node["uuid"] = json!(format!("u-{id}"));
    node["lastMessage"]["id"] = json!(format!("last-{id}"));
    node["lastMessage"]["chatId"] = json!(id);
    node["viewerState"]["list"] = json!(list);
    node["viewerState"]["lists"] = json!([list]);
    node
}

fn page_of(nodes: Vec<Value>) -> Value {
    let total = nodes.len();
    let edges: Vec<Value> = nodes.into_iter().enumerate().map(|(index, node)| json!({"cursor": format!("k{index}"), "node": node})).collect();
    json!({"viewer": {"chats": {"edges": edges, "pageInfo": {"hasNextPage": false, "endCursor": null}, "totalCount": total}}})
}

fn three(middle: Value) -> Value {
    page_of(vec![node_of("c1", "PRIMARY"), middle, node_of("c3", "PRIMARY")])
}

async fn refresh_list(stand: &Stand, list: &str) {
    let epoch = stand.database.session_epoch();
    bound(stand.database.refresh(epoch, "Chat".into(), Some("list".into()), json!({"list": list}).to_string())).await.unwrap();
    stand.settle().await;
}

async fn assert_primary_holds_three(stand: &Stand, what: &str) {
    assert_eq!(local_ids(stand, "PRIMARY").await, ["c1", "c2", "c3"], "{what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_chat_list_keeps_a_row_with_a_media_message_without_size() {
    let stand = Stand::open().await;
    let mut middle = node_of("c2", "PRIMARY");
    middle["lastMessage"]["kind"] = json!("IMAGE");
    middle["lastMessage"]["body"] = Value::Null;
    for field in ["width", "height", "duration"] { middle["lastMessage"]["media"][field] = Value::Null; }
    stand.answer("chats", three(middle));
    refresh_list(&stand, "PRIMARY").await;
    assert_primary_holds_three(&stand, "an IMAGE last message with null width, height, duration and body").await;
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_chat_list_keeps_a_row_with_a_plain_integer_summary_id() {
    let stand = Stand::open().await;
    let mut middle = node_of("c2", "PRIMARY");
    middle["summary"]["id"] = json!("2");
    stand.answer("chats", three(middle));
    refresh_list(&stand, "PRIMARY").await;
    assert_primary_holds_three(&stand, "summary.id 2").await;
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_chat_list_keeps_a_row_that_was_local_in_another_list() {
    let stand = Stand::open().await;
    stand.answer("chats", page_of(vec![node_of("c2", "SECONDARY")]));
    refresh_list(&stand, "SECONDARY").await;
    assert_eq!(local_ids(&stand, "SECONDARY").await, ["c2"], "the setup landed in SECONDARY");
    stand.answer("chats", three(node_of("c2", "PRIMARY")));
    refresh_list(&stand, "PRIMARY").await;
    assert_primary_holds_three(&stand, "c2 moved from SECONDARY to PRIMARY").await;
    assert_eq!(local_ids(&stand, "SECONDARY").await, Vec::<String>::new(), "c2 left SECONDARY");
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_chat_list_keeps_a_row_that_first_came_nested_with_fewer_fields() {
    let stand = Stand::open().await;
    stand.answer("ChatNode", json!({"node": super::stand::chat("c2", "nested")}));
    let rows = stand.entity("Chat", "c2", crate::ffi::ReadFetch::Stale);
    settled(&stand, "the nested row landed", || rows.len() == 1).await;
    stand.answer("chats", three(node_of("c2", "PRIMARY")));
    refresh_list(&stand, "PRIMARY").await;
    assert_primary_holds_three(&stand, "c2 arrived first through a node read").await;
    stand.database.close_now();
}
