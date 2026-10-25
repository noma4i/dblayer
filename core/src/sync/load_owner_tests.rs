use std::time::Duration;

use serde_json::Map;

use super::load::{LoadOptions, LoadStatus};
use super::load_machine::{LoadPhase, RelationState};
use super::rig::*;
use super::SyncError;
use crate::transport::Reply;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

async fn feed_state(rig: &Rig) -> super::RelationWatch {
    rig.sync.relation_state("moments", "feed", &none()).await.unwrap()
}

fn load(rig: &Rig, force: bool) -> tokio::task::JoinHandle<Result<super::load::LoadOutcome, SyncError>> {
    let sync = rig.sync.clone();
    tokio::spawn(async move { sync.load("moments", "feed", &none(), LoadOptions { force }).await })
}

fn load_more(rig: &Rig) -> tokio::task::JoinHandle<Result<super::load::LoadOutcome, SyncError>> {
    let sync = rig.sync.clone();
    tokio::spawn(async move { sync.load_more("moments", "feed", &none()).await })
}

fn summary(state: &RelationState) -> (LoadPhase, Option<&'static str>) {
    (state.phase, state.error.as_ref().map(|error| error.kind))
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_05_a_load_offline_before_the_flight_ends_with_a_network_error() {
    let rig = rig();
    let state = feed_state(&rig).await;
    rig.sync.set_online(false);
    let result = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await;
    assert_eq!(result.unwrap_err(), SyncError::Offline);
    assert_eq!(summary(&state.borrow()), (LoadPhase::Failed, Some("network")));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 0);
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_05_an_offline_load_of_a_window_with_rows_keeps_the_rows_phase_and_shows_the_error() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    let state = feed_state(&rig).await;
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.sync.set_online(false);
    assert!(rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await.is_err());
    assert_eq!(summary(&state.borrow()), (LoadPhase::Loaded, Some("network")));
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_05_going_offline_in_the_middle_of_a_flight_ends_it_with_a_network_error() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1").after(Duration::from_secs(30)));
    let state = feed_state(&rig).await;
    let flight = load(&rig, false);
    rig.transport.await_calls("MomentsFeed", 1).await;
    assert_eq!(state.borrow().phase, LoadPhase::Loading);
    rig.sync.set_online(false);
    assert_eq!(flight.await.unwrap().unwrap_err(), SyncError::Offline);
    assert_eq!(summary(&state.borrow()), (LoadPhase::Failed, Some("network")));
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_06_an_answer_that_arrives_after_the_caller_left_changes_nothing() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1").after(Duration::from_secs(30)));
    let state = feed_state(&rig).await;
    let flight = load(&rig, false);
    rig.transport.await_calls("MomentsFeed", 1).await;
    assert_eq!(state.borrow().phase, LoadPhase::Loading);
    flight.abort();
    let _ = flight.await;
    assert_eq!(summary(&state.borrow()), (LoadPhase::NotLoaded, None));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(summary(&state.borrow()), (LoadPhase::NotLoaded, None));
    assert!(member_ids(&rig, "moments", "feed", &none()).is_empty());
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(summary(&state.borrow()), (LoadPhase::Loaded, None));
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_07_a_window_with_two_flights_stays_loading_until_both_end_in_either_order() {
    for (refresh_takes, more_takes) in [(10, 20), (20, 10)] {
        let rig = rig();
        rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], true, "c1"));
        rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], true, "c1").after(Duration::from_secs(refresh_takes)));
        rig.transport.enqueue("MomentsFeed", feed_reply(&["b"], false, "c2").after(Duration::from_secs(more_takes)));
        let state = feed_state(&rig).await;
        rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
        let refresh = load(&rig, true);
        rig.transport.await_calls("MomentsFeed", 2).await;
        let more = load_more(&rig);
        rig.transport.await_calls("MomentsFeed", 3).await;
        let running = |state: &RelationState| (state.phase, state.refreshing, state.fetching_more);
        assert_eq!(running(&state.borrow()), (LoadPhase::Loading, true, true), "{refresh_takes}/{more_takes}");
        let (first, second) = if refresh_takes < more_takes { (refresh, more) } else { (more, refresh) };
        first.await.unwrap().unwrap();
        let after_first = state.borrow().clone();
        assert_eq!(after_first.phase, LoadPhase::Loading, "{refresh_takes}/{more_takes}");
        assert_eq!(after_first.refreshing && after_first.fetching_more, false);
        second.await.unwrap().unwrap();
        let done = state.borrow().clone();
        assert_eq!((done.phase, done.refreshing, done.fetching_more, done.error), (LoadPhase::Loaded, false, false, None), "{refresh_takes}/{more_takes}");
    }
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_08_going_online_repeats_the_load_that_failed_by_network_once() {
    let rig = rig();
    let state = feed_state(&rig).await;
    rig.sync.set_online(false);
    assert!(rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.is_err());
    assert_eq!(state.borrow().phase, LoadPhase::Failed);
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    rig.sync.set_online(true);
    await_state(&state, |state| summary(state) == (LoadPhase::Loaded, None)).await;
    assert_eq!(summary(&state.borrow()), (LoadPhase::Loaded, None));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
    rig.sync.set_online(false);
    rig.sync.set_online(true);
    await_tasks_ended().await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1, "a healed window is not loaded again");
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_08_going_online_skips_a_window_nobody_observes_and_a_server_error() {
    let rig = rig();
    let watched = feed_state(&rig).await;
    rig.sync.set_online(false);
    assert!(rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.is_err());
    assert!(rig.sync.load("moments", "history", &none(), LoadOptions::default()).await.is_err());
    drop(watched);
    rig.sync.set_online(true);
    await_tasks_ended().await;
    assert_eq!(rig.transport.call_count("MomentsFeed") + rig.transport.call_count("MomentsHistory"), 0);

    let server = super::rig::rig();
    let state = feed_state(&server).await;
    server.transport.enqueue("MomentsFeed", Reply::graphql_error("nope", "FORBIDDEN"));
    assert!(server.sync.load("moments", "feed", &none(), LoadOptions::default()).await.is_err());
    server.sync.set_online(false);
    server.sync.set_online(true);
    await_tasks_ended().await;
    assert_eq!(server.transport.call_count("MomentsFeed"), 1);
    assert_eq!(summary(&state.borrow()), (LoadPhase::Failed, Some("server")));
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_04_a_background_failure_with_rows_is_loaded_with_the_error() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    rig.transport.enqueue("MomentsFeed", Reply::graphql_error("nope", "FORBIDDEN"));
    let state = feed_state(&rig).await;
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.clock.advance(Duration::from_secs(400));
    let kept = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(kept.status, LoadStatus::KeptStale);
    assert_eq!(summary(&state.borrow()), (LoadPhase::Loaded, Some("server")));
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_09_a_cache_reset_returns_the_window_to_not_loaded() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    let state = feed_state(&rig).await;
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(state.borrow().phase, LoadPhase::Loaded);
    rig.db.reset(&["moments"]).unwrap();
    assert_eq!(summary(&state.borrow()), (LoadPhase::NotLoaded, None));
    assert_eq!(state.borrow().loaded_at, None);
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_01_closing_the_last_sync_handle_ends_every_window_as_closed() {
    let rig = rig();
    let state = feed_state(&rig).await;
    let Rig { sync, .. } = rig;
    drop(sync);
    assert_eq!(state.borrow().phase, LoadPhase::Closed);
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_10_the_first_load_after_a_reopen_is_not_superseded_by_the_stored_window() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    let reopened = rig_on(rig.db.clone(), rig.transport.clone(), Default::default());
    reopened.clock.advance(Duration::from_secs(400));
    let outcome = reopened.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(outcome.status, LoadStatus::Loaded);
}
