use super::load::LoadOptions;
use super::rig::*;
use super::SyncConfig;
use crate::pacing::ResumeDrain;
use serde_json::{json, Map};
use std::time::Duration;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[derive(Clone, Copy)]
struct Window {
    name: &'static str,
    loaded_before: bool,
    watched: bool,
    invalidated: bool,
    idle_secs: u64,
    chunks: usize,
    loaded: usize,
    calls: usize,
}

#[tokio::test(start_paused = true)]
async fn resume_reloads_a_watched_window_that_is_stale_invalidated_or_never_loaded() {
    let cases = [
        Window { name: "stale", loaded_before: true, watched: true, invalidated: false, idle_secs: 120, chunks: 1, loaded: 1, calls: 2 },
        Window { name: "recent", loaded_before: true, watched: true, invalidated: false, idle_secs: 30, chunks: 0, loaded: 0, calls: 1 },
        Window { name: "no readers", loaded_before: true, watched: false, invalidated: false, idle_secs: 120, chunks: 0, loaded: 0, calls: 1 },
        Window { name: "invalidated while recent", loaded_before: true, watched: true, invalidated: true, idle_secs: 0, chunks: 1, loaded: 1, calls: 2 },
        Window { name: "never loaded", loaded_before: false, watched: true, invalidated: false, idle_secs: 0, chunks: 1, loaded: 1, calls: 1 },
    ];
    for case in cases {
        let rig = rig();
        rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
        let watch = rig.sync.watch("moments", "feed", &none()).unwrap();
        if case.loaded_before {
            rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
        }
        if !case.watched {
            drop(watch);
        }
        if case.invalidated {
            rig.db.shared.store.invalidate_family("moments", "feed").unwrap();
        }
        rig.clock.advance(Duration::from_secs(case.idle_secs));
        let report = rig.sync.resume().await.unwrap();
        assert_eq!((report.chunks, report.loaded, report.failed), (case.chunks, case.loaded, 0), "{}", case.name);
        assert_eq!(rig.transport.call_count("MomentsFeed"), case.calls, "{}", case.name);
    }
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
