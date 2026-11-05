use std::collections::BTreeMap;
use std::time::Duration;

use std::sync::{Arc, Mutex};

use serde_json::json;

use super::rig::*;
use super::SyncConfig;
use crate::db::Db;
use crate::schema::{Coverage, PreloadChoice, PreloadSource, RelationSpec, RemoteShape};
use crate::sync::OperationDef;
use crate::transport::{MockTransport, Reply};

fn preloading() -> Rig {
    let mut moments = moments_spec();
    for relation in &mut moments.relations {
        match relation.name.as_str() {
            "feed" => relation.preload = Some(BTreeMap::new()),
            "vibe_feed" => relation.preload = Some(BTreeMap::from([("vibeId".to_string(), crate::schema::PreloadChoice::Listed(vec![json!("a"), json!("b")]))])),
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

#[test]
fn an_open_with_a_session_loads_each_declared_window_once() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    let mut vibes: Vec<String> = rig.transport.calls().iter().filter_map(|call| call.variables["vibeId"].as_str().map(str::to_string)).collect();
    vibes.sort();
    assert_eq!(vibes, ["a", "b"]);
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}) }

#[test]
fn a_new_session_starts_the_load_and_the_same_session_does_not() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.transport.set_session(None);
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    rig.transport.set_session(Some("first"));
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.clock.advance(Duration::from_secs(600));
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.transport.set_session(Some("second"));
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 6);
}) }

#[test]
fn a_reset_loads_the_declared_windows_again() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    rig.db.reset(&["moments"]).unwrap();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 6);
}) }

#[test]
fn a_fresh_window_is_not_loaded() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    rig.sync.inner.preload_on_event();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.clock.advance(Duration::from_secs(301));
    rig.sync.inner.preload_on_event();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 6);
}) }

#[test]
fn offline_waits_for_the_online_event() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.sync.set_online(false);
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    rig.sync.set_online(true);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.sync.set_online(false);
    rig.sync.set_online(true);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}) }

#[test]
fn a_failed_session_read_shows_in_the_window_and_the_load_starts_when_it_recovers() { crate::test_wait::paused(async {
    let rig = preloading();
    let window = rig.sync.relation_state("moments", "feed", &serde_json::Map::new()).await.unwrap();
    rig.transport.fail_session("session storage is locked");
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    assert!(window.borrow().error.as_ref().is_some_and(|error| error.message.contains("session storage is locked")));
    rig.transport.set_session(Some("first"));
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}) }

#[test]
fn an_event_while_signed_out_loads_nothing_until_the_session_appears() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.transport.set_session(None);
    rig.sync.commit_session_for_test();
    rig.sync.inner.preload_on_event();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    rig.transport.set_session(Some("first"));
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}) }

#[test]
fn going_offline_during_a_round_ends_it_and_the_online_event_loads_again() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1").after(Duration::from_secs(10)));
    rig.sync.commit_session_for_test();
    crate::test_wait::pass(Duration::from_millis(1)).await;
    rig.sync.set_online(false);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.sync.set_online(true);
    crate::test_wait::pass(Duration::from_secs(11)).await;
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 5);
}) }

#[test]
fn power_saving_holds_the_load_until_it_ends() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.sync.set_power_saving(true);
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    rig.sync.set_online(false);
    rig.sync.set_online(true);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    rig.sync.set_power_saving(false);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
    rig.sync.set_power_saving(true);
    rig.sync.set_power_saving(false);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}) }

#[test]
fn a_low_ram_device_never_loads() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.sync.set_low_ram(true);
    rig.sync.commit_session_for_test();
    rig.sync.set_online(false);
    rig.sync.set_online(true);
    rig.sync.set_power_saving(true);
    rig.sync.set_power_saving(false);
    rig.sync.inner.preload_on_event();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
}) }

#[test]
fn a_round_in_flight_stops_taking_work_when_power_saving_starts() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.sync.set_power_saving(true);
    rig.sync.inner.preload_on_event();
    assert!(!rig.sync.preload_taken());
    assert_eq!(requests(&rig), 0);
    rig.sync.set_power_saving(false);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}) }

fn deriving() -> (Rig, Arc<Mutex<Vec<&'static str>>>) {
    let mut moments = moments_spec();
    for relation in moments.relations.iter_mut().filter(|relation| relation.name == "vibe_feed") {
        let source = PreloadSource { model: "vibes".to_string(), relation: "list".to_string() };
        relation.preload = Some(BTreeMap::from([("vibeId".to_string(), PreloadChoice::RowsOf { from: source })]));
        relation.preload_limit = Some(40);
    }
    let mut vibes = vibes_spec();
    let mut list = RelationSpec::global("list");
    list.remote = Some(remote("VibesList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    list.preload = Some(BTreeMap::new());
    vibes.relations.push(list);
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments, vibes], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("VibesList", "operation VibesList { x }", "vibes"));
    let ids = Arc::new(Mutex::new(vec!["a", "b"]));
    let served = ids.clone();
    rig.transport.respond_with(move |request| {
        if request.operation_name == "VibesList" {
            let nodes: Vec<_> = served.lock().unwrap().iter().map(|id| json!({"id": id})).collect();
            return Some(Reply::data(json!({"vibes": nodes})));
        }
        Some(feed_reply(&["m1"], false, "c1"))
    });
    (rig, ids)
}

fn feed_vibes(rig: &Rig) -> Vec<String> {
    let mut vibes: Vec<String> = rig.transport.calls().iter().filter_map(|call| call.variables["vibeId"].as_str().map(str::to_string)).collect();
    vibes.sort();
    vibes
}

#[test]
fn preload_limited_source() { crate::test_wait::paused(async {
    let mut moments = moments_spec();
    let relation = moments.relations.iter_mut().find(|relation| relation.name == "vibe_feed").unwrap();
    relation.text_params.push("channel".into());
    relation.preload = Some(BTreeMap::from([
        ("vibeId".into(), PreloadChoice::RowsOf { from: PreloadSource { model: "vibes".into(), relation: "list".into() } }),
        ("channel".into(), PreloadChoice::Listed(vec![json!("x"), json!("y")])),
    ]));
    relation.preload_limit = Some(40);
    relation.preload_order = vec!["vibeId".into(), "channel".into()];
    let mut vibes = vibes_spec();
    let mut list = RelationSpec::global("list");
    list.remote = Some(remote("VibesList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    vibes.relations.push(list);
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments, vibes], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("VibesList", "operation VibesList { x }", "vibes"));
    let rows: Vec<_> = (0..10_000).map(|n| params(json!({"id": format!("v{n:05}")}))).collect();
    rig.transport.enqueue("VibesList", Reply::data(json!({"vibes": rows.clone()})));
    rig.transport.enqueue("VibesList", Reply::data(json!({"vibes": rows.into_iter().skip(1).collect::<Vec<_>>()})));
    rig.sync.load(&rig.db.admit().unwrap(), "vibes", "list", &Default::default(), crate::sync::LoadOptions { force: true }).await.unwrap();
    rig.transport.respond_with(|request| (request.operation_name != "VibesList").then(|| feed_reply(&["m1"], false, "c1")));
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 40, "limit covers the product, not each parameter");
    let read_before = crate::identity::touches::member_reads();
    assert_eq!(rig.sync.inner.preloaded_windows().unwrap().len(), 40);
    assert_eq!(crate::identity::touches::member_reads() - read_before, 40, "only the bounded source prefix is read from SQLite");
    let selected = feed_vibes(&rig);
    assert_eq!(selected, (0..20).flat_map(|n| [format!("v{n:05}"), format!("v{n:05}")]).collect::<Vec<_>>(), "declaration order chooses the first 20 sources twice");
    rig.sync.load(&rig.db.admit().unwrap(), "vibes", "list", &Default::default(), crate::sync::LoadOptions { force: true }).await.unwrap();
    rig.sync.inner.preload_on_source("vibes", "list");
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 42, "only the two incoming windows reach the transport");
    assert_eq!(feed_vibes(&rig).iter().filter(|id| id.as_str() == "v00020").count(), 2);
}) }

#[test]
fn each_row_of_a_source_list_gets_one_scope_and_a_new_row_adds_one() { crate::test_wait::paused(async {
    let (rig, ids) = deriving();
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(rig.transport.call_count("VibesList"), 1);
    assert_eq!(feed_vibes(&rig), ["a", "b"]);
    rig.sync.commit_session_for_test();
    rig.sync.inner.preload_on_event();
    rig.sync.preload_settled().await;
    assert_eq!(feed_vibes(&rig), ["a", "b"]);
    ids.lock().unwrap().push("c");
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "vibes", "list", &serde_json::Map::new(), crate::sync::LoadOptions { force: true }).await.unwrap();
    rig.sync.preload_settled().await;
    assert_eq!(feed_vibes(&rig), ["a", "b", "c"]);
    assert_eq!(rig.transport.call_count("VibesList"), 2);
}) }

#[test]
fn nothing_is_fetched_before_the_first_screen_shows_a_snapshot() { crate::test_wait::paused(async {
    let rig = preloading();
    let screen = rig.sync.first_screen();
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    screen.delivered();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}) }

#[test]
fn an_app_load_in_flight_holds_the_start_until_it_ends() { crate::test_wait::paused(async {
    let rig = preloading();
    let app = rig.sync.app_load();
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 0);
    drop(app);
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 3);
}) }

#[test]
fn an_app_load_started_during_the_round_is_served_before_the_queued_preload_fetches() { crate::test_wait::paused(async {
    let mut moments = moments_spec();
    for relation in moments.relations.iter_mut().filter(|relation| relation.name == "vibe_feed") {
        let values = ["a", "b", "c", "d", "e"].map(|id| json!(id)).to_vec();
        relation.preload = Some(BTreeMap::from([("vibeId".to_string(), PreloadChoice::Listed(values))]));
    }
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments, vibes_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.transport.respond_with(|request| {
        let delay = if request.variables["vibeId"] == "app" { 20 } else { 10 };
        Some(feed_reply(&["m1"], false, "c1").after(Duration::from_secs(delay)))
    });
    rig.sync.commit_session_for_test();
    crate::test_wait::pass(Duration::from_millis(1)).await;
    assert_eq!(requests(&rig), 3, "the round runs three fetches at once");
    let sync = rig.sync.clone();
    let app = tokio::spawn(async move { sync.load(&sync.inner.db.admit().unwrap(), "moments", "vibe_feed", &params(json!({"vibeId": "app"})), crate::sync::LoadOptions::default()).await });
    crate::test_wait::pass(Duration::from_secs(15)).await;
    assert_eq!(requests(&rig), 4, "the queued preload fetches wait while the app load is in flight");
    assert!(feed_vibes(&rig).contains(&"app".to_string()));
    app.await.unwrap().unwrap();
    crate::test_wait::pass(Duration::from_secs(30)).await;
    rig.sync.preload_settled().await;
    assert_eq!(requests(&rig), 6);
}) }

#[test]
fn refreshing_preloads_fetches_every_scope_once_even_when_all_are_fresh() { crate::test_wait::paused(async {
    let (rig, ids) = deriving();
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    assert_eq!((rig.transport.call_count("VibesList"), requests(&rig)), (1, 2));
    ids.lock().unwrap().push("c");
    assert!(rig.sync.refresh_preloads(&rig.db.admit().unwrap()).await.unwrap().is_empty());
    assert_eq!(rig.transport.call_count("VibesList"), 2);
    let calls: Vec<String> = rig.transport.calls().iter().filter_map(|call| call.variables["vibeId"].as_str().map(str::to_string)).skip(2).collect();
    let mut refreshed = calls.clone();
    refreshed.sort();
    assert_eq!(refreshed, ["a", "b", "c"], "each scope, the one of the new row too, is fetched once");
}) }

#[test]
fn refreshing_preloads_offline_gives_the_error_and_sends_nothing() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.sync.commit_session_for_test();
    rig.sync.preload_settled().await;
    let before = rig.transport.calls().len();
    rig.sync.set_online(false);
    assert_eq!(rig.sync.refresh_preloads(&rig.db.admit().unwrap()).await.unwrap_err(), crate::sync::SyncError::Offline);
    assert_eq!(rig.transport.calls().len(), before);
}) }

#[test]
fn refreshing_preloads_names_the_scopes_that_failed() { crate::test_wait::paused(async {
    let rig = preloading();
    rig.transport.respond_with(|request| Some(if request.variables["vibeId"] == "b" { Reply::graphql_messages(&["boom"]) } else { feed_reply(&["m1"], false, "c1") }));
    let failures = rig.sync.refresh_preloads(&rig.db.admit().unwrap()).await.unwrap();
    assert_eq!(requests(&rig), 3);
    assert_eq!(failures.len(), 1);
    assert_eq!((failures[0].model.as_str(), failures[0].relation.as_str(), failures[0].params["vibeId"].as_str()), ("moments", "vibe_feed", Some("b")));
}) }

/// E01: a refresh that was admitted in one epoch sends nothing and writes nothing once a session started in another epoch, also for the load that still waits for a lane.
#[test]
fn a_refresh_of_an_epoch_that_left_sends_nothing_from_the_load_waiting_for_a_lane() { crate::test_wait::paused(async {
    let mut moments = moments_spec();
    for relation in &mut moments.relations {
        if relation.name == "vibe_feed" {
            relation.preload = Some(BTreeMap::from([("vibeId".to_string(), PreloadChoice::Listed(["a", "b", "c", "d"].map(|vibe| json!(vibe)).to_vec()))]));
        }
    }
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments, vibes_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.transport.respond_with(|_| Some(feed_reply(&["m1"], false, "c1").after(Duration::from_secs(10))));
    let life = rig.db.shared.store.lifecycle.clone();
    let epoch = life.snapshot().session_epoch;
    let admitted = rig.db.admit().unwrap();
    let refreshing = { let sync = rig.sync.clone(); async move { sync.refresh_preloads(&admitted).await } };
    let running = tokio::spawn(refreshing);
    crate::test_wait::pass(Duration::from_secs(1)).await;
    assert_eq!(requests(&rig), 3, "three loads hold the lanes and the fourth waits");
    rig.sync.set_preload(false);
    assert!(life.start_session_in_tests(Some("another".into()), life.admit_passive(crate::ffi::legacy::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok(), "the session of the other epoch did not start");
    crate::test_wait::pass(Duration::from_secs(60)).await;
    let _ = running.await;
    assert_eq!(requests(&rig), 3, "the load that waited for a lane sent its request in the new epoch");
    assert_eq!(rig.db.shared.store.row_count("moments").unwrap(), 0, "an answer of the epoch that left was written");
}) }
