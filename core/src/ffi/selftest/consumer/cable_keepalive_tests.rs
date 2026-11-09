use std::sync::{Arc, Mutex};

use super::stand::Stand;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::server_contract::CableContract;

async fn open(answer_ping: bool) -> (Stand, Arc<Mutex<CableContract>>) {
    let contract = Arc::new(Mutex::new(CableContract { answer_ping, ..Default::default() }));
    let stand = Stand::prepare_contract(None, Arc::new(WatchedHeld::default()), Some(contract.clone())).await;
    stand.sign_in().await;
    stand.settle().await;
    (stand, contract)
}

fn count(contract: &Mutex<CableContract>, kind: &str) -> usize {
    contract.lock().unwrap().frames.iter().filter(|frame| frame["t"] == kind).count()
}

async fn advance(stand: &Stand, millis: u64) {
    stand.clock.advance(millis);
    stand.settle().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cable_keepalive_ping_pong_prevents_idle_close() {
    let (stand, contract) = open(true).await;
    let (_stream, token) = stand.events("Moment").await;
    stand.settle().await;
    advance(&stand, 24_999).await;
    assert_eq!(count(&contract, "ping"), 0);
    advance(&stand, 1).await;
    assert_eq!(count(&contract, "ping"), 1);
    advance(&stand, 10_000).await;
    assert_eq!(stand.cable_connects(), 1);
    advance(&stand, 15_000).await;
    assert_eq!(count(&contract, "ping"), 2);
    advance(&stand, 10_000).await;
    stand.expire_cable(&token);
    assert!(contract.lock().unwrap().closed.is_empty());
    assert_eq!(stand.cable_connects(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn cable_keepalive_missing_pong_reconnects() {
    let (stand, contract) = open(false).await;
    let (_stream, _token) = stand.events("Moment").await;
    stand.settle().await;
    advance(&stand, 25_000).await;
    assert_eq!(count(&contract, "ping"), 1);
    advance(&stand, 9_999).await;
    assert_eq!(stand.cable_connects(), 1);
    advance(&stand, 1).await;
    assert!(stand.cable_connects() > 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn cable_keepalive_outgoing_frame_rearms() {
    let (stand, contract) = open(true).await;
    let (_stream, token) = stand.events("Moment").await;
    stand.settle().await;
    advance(&stand, 20_000).await;
    stand.cable_ping(&token);
    stand.settle().await;
    assert_eq!(count(&contract, "pong"), 1);
    advance(&stand, 5_000).await;
    assert_eq!(count(&contract, "ping"), 0);
    advance(&stand, 20_000).await;
    assert_eq!(count(&contract, "ping"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn cable_keepalive_idle_has_no_wakeups() {
    let (stand, contract) = open(true).await;
    let armed = stand.clock.armed();
    let connects = stand.cable_connects();
    advance(&stand, 60_000).await;
    assert_eq!(stand.clock.armed(), armed);
    assert_eq!(stand.cable_connects(), connects);
    assert_eq!(count(&contract, "ping"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn cable_keepalive_old_connection_timer_is_ignored() {
    let (stand, contract) = open(true).await;
    let (stream, _) = stand.events("Moment").await;
    stand.settle().await;
    advance(&stand, 20_000).await;
    stream.cancel().await;
    stand.settle().await;
    let connects = stand.cable_connects();
    advance(&stand, 40_000).await;
    assert_eq!(stand.cable_connects(), connects);
    assert_eq!(count(&contract, "ping"), 0);
}

#[test]
fn cable_keepalive_contract_closes_silent_connection() {
    let mut contract = CableContract::default();
    contract.receive(1_000, &serde_json::json!({"t":"subscribe"}));
    assert!(!contract.expire(60_999));
    assert!(contract.expire(61_000));
    assert_eq!(contract.closed, [4408]);
}
