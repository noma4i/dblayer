use serde_json::{json, Value};

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;

fn location() -> Value { json!({"userLocationUpdate": {"location": {"address": "a", "city": "c", "lat": 1.0, "lng": 2.0, "name": "n"}, "viewer": {"user": {"id": "u1", "updatedAt": "2026-11-09T00:00:00Z"}}}}) }

fn register() -> Value { json!({"deviceRegister": {"viewer": {"user": {"id": "u1"}}}}) }

async fn run(stand: &Stand, operation: &str, variables: Value) -> Result<String, crate::ffi::DbError> {
    let epoch = stand.database.session_epoch();
    bound(stand.database.run(epoch, operation.into(), variables.to_string())).await
}

fn write_sequences(stand: &Stand) -> Vec<(String, i64)> { stand.sequenced().into_iter().filter(|(operation, sequence)| operation != "login_link" && *sequence > 0).collect() }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_writes_land_on_the_first_send_on_a_server_with_an_empty_registry() {
    let stand = Stand::open().await;
    stand.answer("UserLocationUpdate", location());
    stand.answer("DeviceRegister", register());
    stand.empty_registry(true);
    run(&stand, "UserLocationUpdate", json!({"input": {"lat": 1.0, "lng": 2.0}})).await.unwrap();
    run(&stand, "DeviceRegister", json!({"input": {"token": "t", "platform": "IOS"}})).await.unwrap();
    run(&stand, "UserLocationUpdate", json!({"input": {"lat": 3.0, "lng": 4.0}})).await.unwrap();
    stand.settle().await;
    let sent = write_sequences(&stand);
    assert_eq!(sent.iter().map(|(operation, _)| operation.as_str()).collect::<Vec<_>>(), ["UserLocationUpdate", "DeviceRegister", "UserLocationUpdate"], "each write went once: {sent:?}");
    let first = sent[0].1;
    assert_eq!(sent.iter().map(|(_, sequence)| *sequence).collect::<Vec<_>>(), [first, first + 1, first + 2], "the numbers grow by 1: {sent:?}");
    assert_eq!(stand.conflicts(), 0, "SEQUENCE_CONFLICT");
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_answer_without_sequence_leaves_the_number_for_the_next_write() {
    let stand = Stand::open().await;
    stand.answer("UserLocationUpdate", location());
    stand.answer("DeviceRegister", register());
    stand.empty_registry(false);
    stand.refuse_once("UserLocationUpdate");
    let _ = run(&stand, "UserLocationUpdate", json!({"input": {"lat": 1.0, "lng": 2.0}})).await;
    run(&stand, "DeviceRegister", json!({"input": {"token": "t", "platform": "IOS"}})).await.unwrap();
    stand.settle().await;
    let sent = write_sequences(&stand);
    let refused = sent.iter().find(|(operation, _)| operation == "UserLocationUpdate").expect("the refused write reached the server").1;
    let landed = sent.iter().find(|(operation, _)| operation == "DeviceRegister").expect("the next write reached the server").1;
    assert_eq!(landed, refused, "the refused number was not spent: {sent:?}");
    assert_eq!(stand.conflicts(), 0, "SEQUENCE_CONFLICT");
}
