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
