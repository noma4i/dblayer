use std::time::Duration;

use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;

#[tokio::test(start_paused = true)]
async fn inv_conc_03_the_send_gate_is_released_when_a_flush_is_cancelled() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    put(&rig, "moments", moment("m2", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.db.update("moments", &rid("m2"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", updated("m1", "new").after(Duration::from_secs(5)));
    rig.transport.enqueue("UpdateMoment", updated("m2", "new"));
    let sync = rig.sync.clone();
    let cancelled = tokio::spawn(async move { sync.flush().await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    cancelled.abort();
    let _ = cancelled.await;
    let next = tokio::time::timeout(Duration::from_secs(60), rig.sync.flush()).await;
    assert!(next.is_ok(), "the next flush waits for a gate nobody holds");
}
