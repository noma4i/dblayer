use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::json;

use super::persisted_query_tests::{documents, request, Rig, WRITE};
use crate::transport::Transport;

async fn lose_the_answer_to_the_second_send(rig: &Rig) {
    let server = rig.server.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    rig.server.on_request(move || {
        if seen.fetch_add(1, Ordering::SeqCst) == 1 {
            server.lose_next_answer();
        }
    });
    super::client_tests::cancel_pending(rig.transport().mutate(&rig.db.admit().unwrap(), request(WRITE)));
    assert_eq!(rig.pending_sequence(), Some(3));
}

#[tokio::test]
async fn the_same_call_after_a_lost_answer_to_the_second_send_returns_the_stored_answer_with_one_effect() {
    let rig = Rig::signed_in().await;
    let before = rig.server.executed().len();
    lose_the_answer_to_the_second_send(&rig).await;

    let answer = rig.transport().mutate(&rig.db.admit().unwrap(), request(WRITE)).await;

    assert_eq!(answer.unwrap().data, Some(json!({ "done": WRITE })));
    assert_eq!(rig.server.executed().len() - before, 1);
    assert_eq!(rig.pending_sequence(), None);
    let sent = rig.graphql_requests();
    assert_eq!(documents(&sent), [false, true, true]);
    assert_eq!(sent[2]["requestId"], sent[1]["requestId"]);
}
