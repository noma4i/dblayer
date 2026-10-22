use std::time::Duration;

use serde_json::json;

use crate::transport::{Reply, TransportError};

use super::super::rig::*;

async fn the_backoff_passes(rig: &Rig) {
    tokio::time::sleep(Duration::from_secs(2)).await;
    rig.clock.advance(Duration::from_secs(2));
}

fn postponed_create(rig: &Rig) -> crate::identity::RowId {
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Network("down".to_string())));
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m77", "text": "hello", "userId": null}})));
    local.id.clone()
}

#[tokio::test(start_paused = true)]
async fn inv_wait_01_settled_parks_once_per_outbox_change_while_a_retry_waits_for_its_backoff() {
    let rig = rig();
    let id = postponed_create(&rig);
    let bounded = tokio::time::timeout(Duration::from_secs(60), rig.sync.settled("moments", &id));
    let (settled, ()) = tokio::join!(bounded, the_backoff_passes(&rig));
    settled.expect("settled did not return within 60 s").unwrap();
    let parks = rig.sync.counters().settle_parks;
    assert!(parks <= 3, "settled parked {parks} times for 1 postponed retry");
}

#[tokio::test(start_paused = true)]
async fn inv_wait_01_answer_parks_once_per_outbox_change_while_a_retry_waits_for_its_backoff() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let id = crate::identity::RowId::new("m1").unwrap();
    let acted = rig.db.act("moments", "star", &id, params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", Reply::fail(TransportError::Network("down".to_string())));
    rig.transport.enqueue("StarMoment", Reply::data(json!({"starMoment": {"moment": {"id": "m1", "text": "t", "userId": null}}})));
    let bounded = tokio::time::timeout(Duration::from_secs(60), rig.sync.answer("moments", acted.operation));
    let (answered, ()) = tokio::join!(bounded, the_backoff_passes(&rig));
    answered.expect("answer did not return within 60 s").unwrap();
    let parks = rig.sync.counters().settle_parks;
    assert!(parks <= 3, "answer parked {parks} times for 1 postponed retry");
}

#[tokio::test(start_paused = true)]
async fn inv_wait_02_a_waiting_caller_wakes_zero_times_in_ten_seconds_of_an_unchanged_outbox() {
    let rig = rig_with(super::super::SyncConfig { send_backoff: crate::pacing::Backoff::new(Duration::from_secs(30), Duration::from_secs(30)), ..Default::default() });
    let id = postponed_create(&rig);
    tokio::select! {
        _ = rig.sync.settled("moments", &id) => panic!("settled returned while the backoff has not passed"),
        () = tokio::time::sleep(Duration::from_secs(10)) => {}
    }
    let counters = rig.sync.counters();
    assert_eq!((counters.settle_parks, counters.transport_calls), (1, 1));
}

#[tokio::test(start_paused = true)]
async fn inv_wait_02_a_waiting_caller_returns_offline_when_the_connection_drops() {
    let rig = rig_with(super::super::SyncConfig { send_backoff: crate::pacing::Backoff::new(Duration::from_secs(30), Duration::from_secs(30)), ..Default::default() });
    let id = postponed_create(&rig);
    let going_offline = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        rig.sync.set_online(false);
    };
    let bounded = tokio::time::timeout(Duration::from_secs(60), rig.sync.settled("moments", &id));
    let (settled, ()) = tokio::join!(bounded, going_offline);
    assert!(matches!(settled, Ok(Err(crate::sync::SyncError::Offline))), "settled did not return Offline within 60 s of going offline");
}

#[test]
fn inv_wait_03_the_send_sources_hold_no_timer() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sync/send");
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with("_tests.rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        for word in ["sleep(", "interval(", "sleep_until("] {
            assert!(!source.contains(word), "{name} holds a timer: {word}");
        }
    }
}

#[test]
fn inv_wait_04_no_test_of_the_wrappers_waits_without_a_bound() {
    let scripts = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../scripts");
    for script in ["check_waits_test.sh", "check_waits.sh"] {
        let output = std::process::Command::new("bash").arg(scripts.join(script)).output().unwrap();
        assert!(output.status.success(), "{script}: {}", String::from_utf8_lossy(&output.stderr));
    }
}
