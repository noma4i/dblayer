use std::time::Duration;

use serde_json::Map;

use super::super::load::{LoadOptions, LoadStatus};
use super::super::rig::*;
use super::super::load_machine::LoadPhase;

fn slow_feed(rig: &Rig) {
    rig.transport.respond_with(|_| Some(feed_reply(&["m1"], false, "c1").after(Duration::from_secs(60))));
}

fn spawn_load(rig: &Rig) -> tokio::task::JoinHandle<Result<super::super::load::LoadOutcome, super::super::SyncError>> {
    let sync = rig.sync.clone();
    tokio::spawn(async move { sync.load(&sync.inner.db.admit().unwrap(), "moments", "feed", &Map::new(), LoadOptions::default()).await })
}

/// The flight is a task of the flight table, not of the caller that started it: the waiting caller keeps it, and its one request answers the waiter (S211).
#[tokio::test(start_paused = true)]
async fn inv_conc_05_a_waiting_caller_survives_the_cancelled_flight_owner() {
    let rig = rig();
    slow_feed(&rig);
    let owner = spawn_load(&rig);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let waiter = spawn_load(&rig);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let before = rig.transport.call_count("MomentsFeed");
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());
    let outcome = waiter.await.unwrap().unwrap();
    assert_eq!(outcome.status, LoadStatus::Loaded);
    assert_eq!((before, rig.transport.call_count("MomentsFeed")), (1, 1), "the request of the cancelled caller answers the waiter");
}

#[tokio::test(start_paused = true)]
async fn inv_conc_05_a_cancelled_flight_owner_leaves_no_loading_state() {
    let rig = rig();
    slow_feed(&rig);
    let state = rig.sync.relation_state("moments", "feed", &Map::new()).await.unwrap();
    let owner = spawn_load(&rig);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(state.borrow().phase, LoadPhase::Loading);
    owner.abort();
    let _ = owner.await;
    let settled = state.borrow().clone();
    assert_eq!((settled.phase, settled.initial, settled.refreshing, settled.fetching_more, settled.error), (LoadPhase::NotLoaded, false, false, false, None));
}

#[tokio::test(start_paused = true)]
async fn inv_conc_05_a_waiting_caller_can_still_be_cancelled_itself() {
    let rig = rig();
    slow_feed(&rig);
    let owner = spawn_load(&rig);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let waiter = spawn_load(&rig);
    tokio::time::sleep(Duration::from_secs(1)).await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(owner.await.unwrap().unwrap().status, LoadStatus::Loaded);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
}
