use super::load::LoadOptions;
use super::rig::*;
use super::state::{LoadPhase, RelationState};
use crate::transport::Reply;
use serde_json::Map;
use std::time::Duration;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[tokio::test(start_paused = true)]
async fn a_relation_never_loaded_is_idle_with_nothing_more() {
    let rig = rig();
    let state = rig.sync.relation_state("moments", "feed", &none()).unwrap();
    assert_eq!(*state.borrow(), RelationState { phase: LoadPhase::Idle, has_more: false, total: None, error: None, loaded_at: None });
}

#[tokio::test(start_paused = true)]
async fn the_state_follows_a_load_from_loading_to_idle_with_the_page_flag() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2").after(Duration::from_secs(1)));
    let state = rig.sync.relation_state("moments", "feed", &none()).unwrap();
    let load = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.load("moments", "feed", &none(), LoadOptions::default()).await }
    });
    settle().await;
    assert_eq!(state.borrow().phase, LoadPhase::Loading);
    load.await.unwrap().unwrap();
    let done = state.borrow().clone();
    assert_eq!((done.phase, done.has_more, done.error.is_none(), done.loaded_at.is_some()), (LoadPhase::Idle, true, true, true));
}

#[tokio::test(start_paused = true)]
async fn load_more_clears_the_page_flag_on_the_last_page() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], true, "c1"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["b"], false, "c2"));
    let state = rig.sync.relation_state("moments", "feed", &none()).unwrap();
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert!(state.borrow().has_more);
    rig.sync.load_more("moments", "feed", &none()).await.unwrap();
    assert!(!state.borrow().has_more);
}

#[tokio::test(start_paused = true)]
async fn the_state_carries_the_total_the_server_reports_for_the_whole_list() {
    let rig = rig();
    let page = |ids: &[&str], cursor: &str, total: i64| {
        let mut data = feed_page(ids.iter().map(|id| moment(id, id)).collect(), true, cursor);
        data["feed"]["totalCount"] = serde_json::json!(total);
        Reply::data(data)
    };
    rig.transport.enqueue("MomentsFeed", page(&["a"], "c1", 7));
    rig.transport.enqueue("MomentsFeed", page(&["b"], "c2", 8));
    let state = rig.sync.relation_state("moments", "feed", &none()).unwrap();
    assert_eq!(state.borrow().total, None);
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(state.borrow().total, Some(7));
    rig.sync.load_more("moments", "feed", &none()).await.unwrap();
    assert_eq!(state.borrow().total, Some(8));
    let reopened = rig.sync.relation_state("moments", "feed", &none()).unwrap();
    assert_eq!(reopened.borrow().total, Some(8));
}

#[tokio::test(start_paused = true)]
async fn a_failed_load_reports_its_kind_and_the_next_success_clears_it() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", Reply::fail(crate::transport::TransportError::Http { status: 422, body: "no".to_string() }));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    let state = rig.sync.relation_state("moments", "feed", &none()).unwrap();
    assert!(rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.is_err());
    let failed = state.borrow().clone();
    assert_eq!(failed.phase, LoadPhase::Failed);
    assert!(failed.error.is_some());
    rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    let healed = state.borrow().clone();
    assert_eq!((healed.phase, healed.error), (LoadPhase::Idle, None));
}

#[tokio::test(start_paused = true)]
async fn a_second_receiver_starts_from_the_stored_window() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], true, "c1"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let fresh = rig.sync.relation_state("moments", "feed", &none()).unwrap();
    assert!(fresh.borrow().has_more && fresh.borrow().loaded_at.is_some());
}
