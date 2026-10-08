use super::error::SyncError;
use super::load::{DiscardReason, LoadOptions, LoadStatus};
use super::support::*;
use crate::read::LoadMoreVerdict;
use crate::transport::Reply;
use serde_json::{json, Map};
use std::time::Duration;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[tokio::test(start_paused = true)]
async fn next_page_uses_the_cursor_and_appends_in_server_order() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], false, "c4"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let outcome = rig.sync.load_more("moments", "feed", &none()).await.unwrap();
    assert_eq!((outcome.status, outcome.rows, outcome.has_next), (LoadStatus::Loaded, 2, false));
    assert_eq!(rig.transport.calls()[1].variables, json!({"first": 2, "after": "c2"}));
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["a", "b", "c", "d"]);
    let key = rig.sync.window_key("moments", "feed", &none()).unwrap();
    let stored = rig.db.shared.store.window(&key).unwrap().unwrap();
    assert_eq!((stored.cursor.as_deref(), stored.has_next, stored.ids.len()), (Some("c4"), false, 4));
}

#[tokio::test(start_paused = true)]
async fn no_next_page_means_no_transport_call() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let outcome = rig.sync.load_more("moments", "feed", &none()).await.unwrap();
    assert_eq!(outcome.status, LoadStatus::NoMorePages(LoadMoreVerdict::NoCursor));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
}

#[tokio::test(start_paused = true)]
async fn load_more_without_a_window_has_no_cursor() {
    let rig = rig();
    let outcome = rig.sync.load_more("moments", "feed", &none()).await.unwrap();
    assert_eq!(outcome.status, LoadStatus::NoMorePages(LoadMoreVerdict::NoCursor));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 0);
}

#[tokio::test(start_paused = true)]
async fn max_pages_stops_the_chain() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], true, "c4"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["e", "f"], true, "c6"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(rig.sync.load_more("moments", "feed", &none()).await.unwrap().status, LoadStatus::Loaded);
    let capped = rig.sync.load_more("moments", "feed", &none()).await.unwrap();
    assert_eq!(capped.status, LoadStatus::NoMorePages(LoadMoreVerdict::MaxPagesReached));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn backward_pages_send_last_and_before() {
    let rig = rig();
    let page = |ids: &[&str], cursor: &str| Reply::data(json!({"history": {"nodes": ids.iter().map(|id| moment(id, id)).collect::<Vec<_>>(), "pageInfo": {"hasNextPage": true, "endCursor": cursor}}}));
    rig.transport.enqueue("MomentsHistory", page(&["n1", "n2"], "h2"));
    rig.transport.enqueue("MomentsHistory", page(&["o1", "o2"], "h4"));
    rig.sync.load("moments", "history", &none(), LoadOptions::default()).await.unwrap();
    rig.sync.load_more("moments", "history", &none()).await.unwrap();
    assert_eq!(rig.transport.calls()[0].variables, json!({"last": 2}));
    assert_eq!(rig.transport.calls()[1].variables, json!({"last": 2, "before": "h2"}));
    assert_eq!(member_ids(&rig, "moments", "history", &none()), ["n1", "n2", "o1", "o2"]);
}

#[tokio::test(start_paused = true)]
async fn load_more_on_unpaged_relation_is_an_error() {
    let rig = rig();
    let result = rig.sync.load_more("moments", "by_user", &params(json!({"userId": "u1"}))).await;
    assert!(matches!(result, Err(SyncError::NotPaged { .. })));
}

#[tokio::test(start_paused = true)]
async fn second_load_more_during_flight_is_already_loading() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], true, "c4").after(Duration::from_secs(3)));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let sync = rig.sync.clone();
    let first = tokio::spawn(async move { sync.load_more("moments", "feed", &Map::new()).await });
    settle().await;
    let second = rig.sync.load_more("moments", "feed", &none()).await.unwrap();
    assert_eq!(second.status, LoadStatus::NoMorePages(LoadMoreVerdict::AlreadyLoading));
    assert_eq!(first.await.unwrap().unwrap().status, LoadStatus::Loaded);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn refresh_during_load_more_discards_the_late_page() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], true, "c4").after(Duration::from_secs(3)));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "r2"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let sync = rig.sync.clone();
    let late = tokio::spawn(async move { sync.load_more("moments", "feed", &Map::new()).await });
    settle().await;
    rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    let outcome = late.await.unwrap().unwrap();
    assert_eq!(outcome.status, LoadStatus::Discarded(DiscardReason::ChainMoved));
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["a", "b"]);
}
