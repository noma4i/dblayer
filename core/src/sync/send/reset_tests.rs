use std::time::Duration;

use serde_json::json;

use crate::identity::RowId;
use crate::transport::{Reply, TransportError};

use super::super::rig::*;

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

async fn reset_during_flush(rig: &Rig, models: &[&str]) {
    let reset = async {
        crate::test_wait::pass(Duration::from_secs(1)).await;
        rig.db.clear_models(models).unwrap();
    };
    let (flushed, ()) = tokio::join!(crate::sync::rig::app(&rig.sync, async { rig.sync.flush(&rig.sync.inner.db.admit().unwrap()).await }), reset);
    flushed.unwrap();
}

fn rig_with_pending_update() -> Rig {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig
}

#[test]
fn inv_op_13_an_answer_a_refusal_or_a_network_failure_after_a_reset_leaves_no_trace_and_no_operation_in_flight() { crate::test_wait::paused(async {
    let answers = [
        (Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})), 1),
        (Reply::graphql_error("not allowed", "FORBIDDEN"), 1),
        (Reply::fail(TransportError::Network("down".to_string())), 2),
    ];
    for models in [&["moments"][..], &["vibes"][..]] {
        for (answer, calls) in answers.clone() {
            let rig = rig_with_pending_update();
            rig.transport.enqueue("UpdateMoment", answer.clone().after(Duration::from_secs(5)));
            rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})));
            reset_during_flush(&rig, models).await;
            let started = rig.db.outbox().unwrap().iter().filter(|entry| entry.operation.transport_started).count();
            assert_eq!(started, 0, "{models:?} {answer:?}");
            rig.clock.advance(Duration::from_secs(3600));
            crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
            assert!(rig.db.outbox().unwrap().is_empty(), "{models:?} {answer:?}");
            let resurrected = models == ["moments"] && text_of(&rig, "moments", "m1").is_some();
            assert!(!resurrected, "{models:?} {answer:?}");
            let expected = if models == ["moments"] { 1 } else { calls };
            assert_eq!(rig.transport.call_count("UpdateMoment"), expected, "{models:?} {answer:?}");
        }
    }
}) }

#[test]
fn a_create_answered_after_a_reset_of_another_model_leaves_no_temp_row_behind() { crate::test_wait::paused(async {
    let rig = rig();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m77", "text": "hello", "userId": null}})).after(Duration::from_secs(5)));
    let reset = async {
        crate::test_wait::pass(Duration::from_secs(1)).await;
        rig.db.clear_models(&["vibes"]).unwrap();
    };
    let (flushed, ()) = tokio::join!(crate::sync::rig::app(&rig.sync, async { rig.sync.flush(&rig.sync.inner.db.admit().unwrap()).await }), reset);
    flushed.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.find("moments", &local.id).unwrap(), None);
}) }
