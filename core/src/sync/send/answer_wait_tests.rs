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
    assert!(locked(&client.sync.inner.answer_roots).len() <= 257, "results that nobody waits for were not bounded");
    assert_eq!(client.sync.answer_root(1), Some(Some(root)), "the result of the waiting call was dropped");
    client.sync.inner.publish_answer_root(1, None);
    drop(wish);
    assert_eq!(client.sync.answer_root(1), None, "a call that went away left its result");
}
