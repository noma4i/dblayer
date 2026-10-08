use serde_json::{json, Map};

use super::super::load::LoadOptions;
use super::super::support::*;
use crate::identity::RowId;
use crate::transport::Reply;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn created() -> Reply {
    Reply::data(json!({"createMoment": {"id": "m9", "text": "t", "userId": null}}))
}

fn invalidated(rig: &Rig, relation: &str) -> bool {
    let key = rig.sync.window_key("moments", relation, &none()).unwrap();
    rig.db.shared.store.window_freshness(&key).unwrap().is_some_and(|freshness| freshness.invalidated)
}

async fn load(rig: &Rig, relation: &str) {
    rig.sync.load("moments", relation, &none(), LoadOptions::default()).await.unwrap();
}

fn history_reply(ids: &[&str]) -> Reply {
    Reply::data(json!({"history": feed_page(ids.iter().map(|id| moment(id, id)).collect(), false, "c")["feed"]}))
}

#[tokio::test(start_paused = true)]
async fn created_moment_marks_the_open_feed_stale_and_the_next_load_reads_it() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    load(&rig, "feed").await;
    rig.db.create("moments", params(json!({"text": "t"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created());
    rig.sync.flush().await.unwrap();
    assert!(invalidated(&rig, "feed"));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
    assert!(!invalidated(&rig, "feed"));
}

#[tokio::test(start_paused = true)]
async fn watched_feed_is_reread_right_after_create() {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m9", "m1"], false, "c2"));
    load(&rig, "feed").await;
    rig.db.create("moments", params(json!({"text": "t"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created());
    rig.sync.flush().await.unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
    assert!(!invalidated(&rig, "feed"));
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m9", "m1"]);
}

#[tokio::test(start_paused = true)]
async fn feed_without_an_open_window_is_not_touched() {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.db.create("moments", params(json!({"text": "t"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created());
    rig.sync.flush().await.unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 0);
}

#[tokio::test(start_paused = true)]
async fn refetch_marks_only_the_named_relations() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsHistory", history_reply(&["m1"]));
    load(&rig, "feed").await;
    load(&rig, "history").await;
    rig.db.act("moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
    rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
    rig.sync.flush().await.unwrap();
    assert!(invalidated(&rig, "history"));
    assert!(!invalidated(&rig, "feed"));
}

#[tokio::test(start_paused = true)]
async fn update_without_refetch_marks_nothing() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    load(&rig, "feed").await;
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})));
    rig.sync.flush().await.unwrap();
    assert!(!invalidated(&rig, "feed"));
}

#[tokio::test(start_paused = true)]
async fn destroy_success_marks_the_feed_stale() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    load(&rig, "feed").await;
    rig.db.destroy("moments", &rid("m1")).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::data(json!({"deleteMoment": {"ok": true}})));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 1);
    assert!(invalidated(&rig, "feed"));
}

#[tokio::test(start_paused = true)]
async fn refresh_rereads_a_fresh_window() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m2"], false, "c2"));
    load(&rig, "feed").await;
    rig.sync.refresh("moments", "feed", &none()).await.unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m2"]);
}
