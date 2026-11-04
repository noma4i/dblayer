use super::error::SyncError;
use super::load::{DiscardReason, LoadOptions, LoadStatus};
use super::rig::*;
use super::SyncConfig;
use crate::db::Db;
use crate::read::LoadMoreVerdict;
use crate::transport::{MockTransport, Reply};
use serde_json::{json, Map};
use std::time::Duration;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

async fn reset_invalid_cursor(rig: &Rig, ids: &[&str]) {
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], true, "c4"));
    rig.transport.enqueue("MomentsFeed", Reply::graphql_error("expired", "INVALID_CURSOR"));
    rig.transport.enqueue("MomentsFeed", feed_reply(ids, true, "n2"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    let outcome = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!((outcome.status, outcome.has_next), (LoadStatus::Loaded, true));
    assert_eq!(rig.transport.calls()[3].variables, json!({"first": 2}));
    assert!(rig.sync.relation_state("moments", "feed", &none()).await.unwrap().borrow().error.is_none());
}

fn cursor_specs() -> Vec<crate::schema::ModelSpec> {
    let mut models = specs();
    let model = models.iter_mut().find(|model| model.key.as_str() == "moments").unwrap();
    let feed = model.relations.iter_mut().find(|relation| relation.name == "feed").unwrap();
    if let crate::schema::RemoteShape::Connection { max_pages, .. } = &mut feed.remote.as_mut().unwrap().shape {
        *max_pages = Some(3);
    }
    models
}

#[test]
fn invalid_cursor_replaces_the_window_and_keeps_nodes() { crate::test_wait::paused(async {
    for ids in [["x", "y"], ["x", "b"]] {
        let rig = rig_on(Db::open_in_memory(cursor_specs(), None).unwrap(), MockTransport::new(), SyncConfig::default());
        reset_invalid_cursor(&rig, &ids).await;
        assert_eq!(member_ids(&rig, "moments", "feed", &none()), ids);
        assert!(text_of(&rig, "moments", "a").is_some());
        let key = rig.sync.window_key("moments", "feed", &none()).unwrap();
        let window = rig.db.shared.store.window(&key).unwrap().unwrap();
        assert_eq!((window.cursor.as_deref(), window.has_next), (Some("n2"), true));
        rig.transport.enqueue("MomentsFeed", feed_reply(&["z"], false, "n3"));
        assert_eq!(rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap().status, LoadStatus::Loaded);
    }
}) }

#[test]
fn invalid_cursor_window_survives_reopening() { crate::test_wait::paused(async {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cursor.sqlite");
    {
        let rig = rig_on(Db::open(&path, cursor_specs(), None).unwrap(), MockTransport::new(), SyncConfig::default());
        reset_invalid_cursor(&rig, &["x", "y"]).await;
    }
    let rig = rig_on(Db::open(&path, cursor_specs(), None).unwrap(), MockTransport::new(), SyncConfig::default());
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["x", "y"]);
    let key = rig.sync.window_key("moments", "feed", &none()).unwrap();
    let window = rig.db.shared.store.window(&key).unwrap().unwrap();
    assert_eq!((window.cursor.as_deref(), window.has_next), (Some("n2"), true));
}) }

#[test]
fn invalid_cursor_on_first_page_is_not_repeated() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", Reply::graphql_error("expired", "INVALID_CURSOR"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["x", "y"], true, "n2"));
    assert!(matches!(rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await, Err(SyncError::Graphql { .. })));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
}) }

#[test]
fn next_page_uses_the_cursor_and_appends_in_server_order() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], false, "c4"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let outcome = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!((outcome.status, outcome.rows, outcome.has_next), (LoadStatus::Loaded, 2, false));
    assert_eq!(rig.transport.calls()[1].variables, json!({"first": 2, "after": "c2"}));
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["a", "b", "c", "d"]);
    let key = rig.sync.window_key("moments", "feed", &none()).unwrap();
    let stored = rig.db.shared.store.window(&key).unwrap().unwrap();
    assert_eq!((stored.cursor.as_deref(), stored.has_next), (Some("c4"), false));
}) }

#[test]
fn inv_fresh_06_no_next_page_means_no_transport_call() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let outcome = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!(outcome.status, LoadStatus::NoMorePages(LoadMoreVerdict::NoCursor));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
}) }

#[test]
fn load_more_without_a_window_has_no_cursor() { crate::test_wait::paused(async {
    let rig = rig();
    let outcome = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!(outcome.status, LoadStatus::NoMorePages(LoadMoreVerdict::NoCursor));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 0);
}) }

#[test]
fn max_pages_stops_the_chain() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], true, "c4"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["e", "f"], true, "c6"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap().status, LoadStatus::Loaded);
    let capped = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!(capped.status, LoadStatus::NoMorePages(LoadMoreVerdict::MaxPagesReached));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}) }

#[test]
fn max_pages_counts_fetched_pages_not_stored_members() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c4"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], true, "c6"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap().status, LoadStatus::Loaded);
    let capped = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!(capped.status, LoadStatus::NoMorePages(LoadMoreVerdict::MaxPagesReached));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}) }

#[test]
fn backward_pages_send_last_and_before() { crate::test_wait::paused(async {
    let rig = rig();
    let page = |ids: &[&str], cursor: &str| Reply::data(json!({"history": {"nodes": ids.iter().map(|id| moment(id, id)).collect::<Vec<_>>(), "pageInfo": {"hasNextPage": true, "endCursor": cursor}}}));
    rig.transport.enqueue("MomentsHistory", page(&["n1", "n2"], "h2"));
    rig.transport.enqueue("MomentsHistory", page(&["o1", "o2"], "h4"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "history", &none(), LoadOptions::default()).await.unwrap();
    rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "history", &none()).await.unwrap();
    assert_eq!(rig.transport.calls()[0].variables, json!({"last": 2}));
    assert_eq!(rig.transport.calls()[1].variables, json!({"last": 2, "before": "h2"}));
    assert_eq!(member_ids(&rig, "moments", "history", &none()), ["n1", "n2", "o1", "o2"]);
}) }

#[test]
fn load_more_on_unpaged_relation_is_an_error() { crate::test_wait::paused(async {
    let rig = rig();
    let result = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "by_user", &params(json!({"userId": "u1"}))).await;
    assert!(matches!(result, Err(SyncError::NotPaged { .. })));
}) }

#[test]
fn inv_fresh_06_second_load_more_during_flight_is_already_loading() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], true, "c4").after(Duration::from_secs(3)));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let sync = rig.sync.clone();
    let first = tokio::spawn(async move { sync.load_more(&sync.inner.db.admit().unwrap(), "moments", "feed", &Map::new()).await });
    rig.transport.await_calls("MomentsFeed", 2).await;
    let second = rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!(second.status, LoadStatus::NoMorePages(LoadMoreVerdict::AlreadyLoading));
    assert_eq!(first.await.unwrap().unwrap().status, LoadStatus::Loaded);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}) }

#[test]
fn refresh_during_load_more_discards_the_late_page() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], true, "c4").after(Duration::from_secs(3)));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "r2"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let sync = rig.sync.clone();
    let late = tokio::spawn(async move { sync.load_more(&sync.inner.db.admit().unwrap(), "moments", "feed", &Map::new()).await });
    rig.transport.await_calls("MomentsFeed", 2).await;
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    let outcome = late.await.unwrap().unwrap();
    assert_eq!(outcome.status, LoadStatus::Discarded(DiscardReason::ChainMoved));
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["a", "b"]);
}) }
