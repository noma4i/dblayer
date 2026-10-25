use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::json;

use super::rig::*;
use super::SyncConfig;
use crate::db::Db;
use crate::transport::MockTransport;

fn preloading() -> Rig {
    let mut moments = moments_spec();
    for relation in &mut moments.relations {
        match relation.name.as_str() {
            "feed" => relation.preload = Some(BTreeMap::new()),
            "vibe_feed" => relation.preload = Some(BTreeMap::from([("vibeId".to_string(), vec![json!("a"), json!("b")])])),
            _ => {}
        }
    }
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments, vibes_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig
}

impl super::Sync {
    async fn preload_settled(&self) {
        for _ in 0..100_000 {
            if !self.preload_running() { return; }
            tokio::task::yield_now().await;
        }
        panic!("the preload round never ended");
    }
}

fn requests(rig: &Rig) -> usize {
    rig.transport.call_count("MomentsFeed")
}

#[tokio::test(start_paused = true)]
async fn an_open_with_a_session_loads_each_declared_window_once() {
    let rig = preloading();
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    let mut vibes: Vec<String> = rig.transport.calls().iter().filter_map(|call| call.variables["vibeId"].as_str().map(str::to_string)).collect();
    vibes.sort();
    assert_eq!(vibes, ["a", "b"]);
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}

#[tokio::test(start_paused = true)]
async fn a_new_session_starts_the_load_and_the_same_session_does_not() {
    let rig = preloading();
    rig.transport.set_session(None);
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    rig.transport.set_session(Some("first"));
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.clock.advance(Duration::from_secs(600));
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.transport.set_session(Some("second"));
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 6);
}

#[tokio::test(start_paused = true)]
async fn a_reset_loads_the_declared_windows_again() {
    let rig = preloading();
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    rig.db.reset(&["moments"]).unwrap();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 6);
}

#[tokio::test(start_paused = true)]
async fn a_fresh_window_is_not_loaded() {
    let rig = preloading();
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    rig.sync.inner.preload_on_event();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.clock.advance(Duration::from_secs(301));
    rig.sync.inner.preload_on_event();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 6);
}

#[tokio::test(start_paused = true)]
async fn offline_waits_for_the_online_event() {
    let rig = preloading();
    rig.sync.set_online(false);
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    rig.sync.set_online(true);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.sync.set_online(false);
    rig.sync.set_online(true);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}

#[tokio::test(start_paused = true)]
async fn a_failed_session_read_shows_in_the_window_and_the_load_starts_when_it_recovers() {
    let rig = preloading();
    let window = rig.sync.relation_state("moments", "feed", &serde_json::Map::new()).await.unwrap();
    rig.transport.fail_session("session storage is locked");
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    assert!(window.borrow().error.as_ref().is_some_and(|error| error.message.contains("session storage is locked")));
    rig.transport.set_session(Some("first"));
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}

#[tokio::test(start_paused = true)]
async fn an_event_while_signed_out_loads_nothing_until_the_session_appears() {
    let rig = preloading();
    rig.transport.set_session(None);
    rig.sync.preload_on_session();
    rig.sync.inner.preload_on_event();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    rig.transport.set_session(Some("first"));
    rig.sync.preload_on_session();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}

#[tokio::test(start_paused = true)]
async fn going_offline_during_a_round_ends_it_and_the_online_event_loads_again() {
    let rig = preloading();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1").after(Duration::from_secs(10)));
    rig.sync.preload_on_session();
    tokio::time::sleep(Duration::from_millis(1)).await;
    rig.sync.set_online(false);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.sync.set_online(true);
    tokio::time::sleep(Duration::from_secs(11)).await;
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 5);
}
