use super::load::LoadOptions;
use super::support::*;
use super::SyncConfig;
use crate::pacing::ResumeDrain;
use serde_json::{json, Map};
use std::time::Duration;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[tokio::test(start_paused = true)]
async fn stale_watched_window_is_reloaded() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.clock.advance(Duration::from_secs(120));
    let report = rig.sync.resume().await.unwrap();
    assert_eq!((report.chunks, report.loaded, report.failed), (1, 1, 0));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn recently_loaded_window_is_left_alone() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.clock.advance(Duration::from_secs(30));
    let report = rig.sync.resume().await.unwrap();
    assert_eq!((report.chunks, report.loaded), (0, 0));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
}

#[tokio::test(start_paused = true)]
async fn window_without_readers_is_not_reloaded() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    let watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    drop(watch);
    rig.clock.advance(Duration::from_secs(120));
    assert_eq!(rig.sync.resume().await.unwrap().loaded, 0);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
}

#[tokio::test(start_paused = true)]
async fn invalidated_window_is_reloaded_even_when_recent() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.db.shared.store.invalidate_family("moments", "feed").unwrap();
    assert_eq!(rig.sync.resume().await.unwrap().loaded, 1);
}

#[tokio::test(start_paused = true)]
async fn never_loaded_watched_window_is_loaded() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    assert_eq!(rig.sync.resume().await.unwrap().loaded, 1);
}

#[tokio::test(start_paused = true)]
async fn windows_are_reloaded_in_chunks() {
    let rig = rig_with(SyncConfig { resume: ResumeDrain::new(Duration::from_secs(60), 2), ..SyncConfig::default() });
    rig.transport.enqueue("MomentsByUser", rig_list(&["a"]));
    let mut watches = Vec::new();
    for user in ["u1", "u2", "u3", "u4", "u5"] {
        watches.push(rig.sync.watch("moments", "by_user", &params(json!({"userId": user}))).unwrap());
    }
    let report = rig.sync.resume().await.unwrap();
    assert_eq!((report.chunks, report.loaded, report.failed), (3, 5, 0));
    assert_eq!(rig.transport.call_count("MomentsByUser"), 5);
}

#[tokio::test(start_paused = true)]
async fn failed_reload_is_counted() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", crate::transport::Reply::graphql_error("nope", "FORBIDDEN"));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    let report = rig.sync.resume().await.unwrap();
    assert_eq!((report.loaded, report.failed), (0, 1));
}

fn rig_list(ids: &[&str]) -> crate::transport::Reply {
    crate::transport::Reply::data(json!({"moments": ids.iter().map(|id| moment(id, id)).collect::<Vec<_>>()}))
}
