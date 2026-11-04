use super::*;
use crate::identity::RowId;

/// D05: the result a waiting call expects stays while more than 256 results that nobody waits for pass, and goes with the call.
#[test]
fn the_answer_a_call_waits_for_survives_256_results_nobody_waits_for() {
    let client = crate::sync::rig::rig();
    let root = RowId::new("answered").unwrap();
    let wish = client.sync.expect_answer(1);
    client.sync.inner.publish_answer_root(1, Some(root.clone()));
    for operation in 2..400 {
        client.sync.inner.publish_answer_root(operation, None);
    }
    assert!(locked(&client.sync.inner.answers).held() <= 257, "results that nobody waits for were not bounded");
    assert_eq!(client.sync.answer_root(1), Some(Some(root)), "the result of the waiting call was dropped");
    client.sync.inner.publish_answer_root(1, None);
    drop(wish);
    assert_eq!(client.sync.answer_root(1), None, "a call that went away left its result");
}

/// E03: the claim on the answer exists before the sender can reach the operation, so no flush can answer and evict it first.
#[test]
fn a_waiting_call_claims_its_operation_before_the_sender_can_see_it() {
    use crate::sync::rig::{moment, params, put};
    let client = crate::sync::rig::rig();
    put(&client, "moments", moment("m1", "t"));
    let queue = client.sync.inner.operation_queue().unwrap().clone();
    let mut announced_at_claim = None;
    let (acted, wish) = client.db.act_claiming(&client.db.admit().unwrap(), "moments", "star", &RowId::new("m1").unwrap(), params(serde_json::json!({})), params(serde_json::json!({})), |operation| {
        announced_at_claim = Some(locked(&queue).entries.contains_key(&operation));
        client.sync.expect_answer(operation)
    }).unwrap();
    assert_eq!(announced_at_claim, Some(false), "the sender could reach the operation before the claim");
    assert!(locked(&queue).entries.contains_key(&acted.operation), "the operation was never announced");
    drop(wish);
}
