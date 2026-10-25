use std::time::Duration;

use serde_json::{json, Map};

use super::load::{DiscardReason, LoadOptions, LoadStatus};
use super::node_tests::{data, node_rig, stored};
use super::rig::*;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

fn reset_everything(rig: &Rig) {
    rig.db.reset_all().unwrap();
}

fn reset_chats_cache(rig: &Rig) {
    rig.db.reset(&["chats"]).unwrap();
}

async fn nodes_answered_after(wipe: fn(&Rig)) {
    let rig = node_rig();
    rig.transport.enqueue("ChatsByIds", data(json!({"nodes": [{"id": "c1", "title": "a"}, {"id": "c2", "title": "b"}]})).after(Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.nodes("chats", "batch", &["c1", "c2"]).await });
    rig.transport.await_calls("ChatsByIds", 1).await;
    wipe(&rig);
    let rows = task.await.unwrap().unwrap();
    assert_eq!((stored(&rig, "chats", "c1"), stored(&rig, "chats", "c2")), (None, None));
    assert!(rows.is_empty());
}

async fn node_answered_after(wipe: fn(&Rig)) {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "a"}})).after(Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.node("chats", "node", "c1", false).await });
    rig.transport.await_calls("ChatNode", 1).await;
    wipe(&rig);
    assert_eq!(task.await.unwrap().unwrap(), None);
    assert_eq!(stored(&rig, "chats", "c1"), None);
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_03_nodes_answer_after_reset_all_lands_nothing() {
    nodes_answered_after(reset_everything).await;
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_03_nodes_answer_after_a_cache_reset_lands_nothing() {
    nodes_answered_after(reset_chats_cache).await;
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_03_node_answer_after_reset_all_lands_nothing() {
    node_answered_after(reset_everything).await;
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_03_node_answer_after_a_cache_reset_lands_nothing() {
    node_answered_after(reset_chats_cache).await;
}

#[tokio::test(start_paused = true)]
async fn nodes_answer_in_the_same_generation_still_lands() {
    let rig = node_rig();
    rig.transport.enqueue("ChatsByIds", data(json!({"nodes": [{"id": "c1", "title": "a"}]})).after(Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.nodes("chats", "batch", &["c1"]).await });
    let rows = task.await.unwrap().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("a"));
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_03_next_page_answer_after_reset_all_lands_nothing() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], false, "c4").after(Duration::from_secs(5)));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.load_more("moments", "feed", &none()).await });
    rig.transport.await_calls("MomentsFeed", 2).await;
    reset_everything(&rig);
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(outcome.status, LoadStatus::Discarded(DiscardReason::StaleGeneration));
    assert_eq!((text_of(&rig, "moments", "c"), text_of(&rig, "moments", "d")), (None, None));
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_03_resume_answer_after_reset_all_lands_nothing() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m2"], false, "c2").after(Duration::from_secs(5)));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.clock.advance(Duration::from_secs(120));
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.resume().await });
    rig.transport.await_calls("MomentsFeed", 2).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
    reset_everything(&rig);
    task.await.unwrap().unwrap();
    assert_eq!((text_of(&rig, "moments", "m1"), text_of(&rig, "moments", "m2")), (None, None));
    assert!(member_ids(&rig, "moments", "feed", &none()).is_empty());
}
