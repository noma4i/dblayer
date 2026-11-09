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
async fn consumer_writes_land_on_the_second_pair_on_a_server_with_an_empty_registry() {
    let stand = Stand::open().await;
    stand.answer("UserLocationUpdate", location());
    stand.answer("DeviceRegister", register());
    stand.empty_registry();
    run(&stand, "UserLocationUpdate", json!({"input": {"lat": 1.0, "lng": 2.0}})).await.unwrap();
    run(&stand, "DeviceRegister", json!({"input": {"token": "t", "platform": "IOS"}})).await.unwrap();
    run(&stand, "UserLocationUpdate", json!({"input": {"lat": 3.0, "lng": 4.0}})).await.unwrap();
    stand.settle().await;
    let sent = write_sequences(&stand);
    let first = sent[0].1;
    assert_eq!(
        sent.iter().map(|(operation, sequence)| (operation.as_str(), *sequence)).collect::<Vec<_>>(),
        [("UserLocationUpdate", first), ("UserLocationUpdate", first), ("DeviceRegister", first + 1), ("DeviceRegister", first + 1), ("UserLocationUpdate", first + 2)],
        "a new document costs a second pair with the same number, a known one lands at once: {sent:?}"
    );
    assert_eq!(stand.conflicts(), 0, "SEQUENCE_CONFLICT");
}
