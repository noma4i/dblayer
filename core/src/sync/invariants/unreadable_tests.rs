use serde_json::{json, Value};

use super::super::rig::*;
use super::fixtures::*;
use crate::operation::quarantine_counts;
use crate::syncstate::SyncState;
use crate::transport::{Reply, TransportError};

fn queue(kind: &str) -> (Rig, &'static str, &'static str) {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    match kind {
        "create" => {
            rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "new"}))).unwrap();
            (rig, "CreateMoment", "createMoment")
        }
        "update" => {
            rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
            (rig, "UpdateMoment", "updateMoment")
        }
        "destroy" => {
            rig.db.destroy(&rig.db.admit().unwrap(), "moments", &rid("m1")).unwrap();
            (rig, "DeleteMoment", "deleteMoment")
        }
        _ => {
            rig.db.act(&rig.db.admit().unwrap(), "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
            (rig, "LikeMoment", "likeMoment")
        }
    }
}

fn unreadable_answers(kind: &str, root: &str) -> Vec<(&'static str, Reply)> {
    let mut answers = vec![
        ("wrong root", Reply::data(json!({"wrong": {"id": "m1"}}))),
        ("string where a row belongs", Reply::data(json!({ root: "garbage" }))),
        ("broken json", Reply::fail(TransportError::Decode("bad json".to_string()))),
    ];
    if kind != "destroy" {
        answers.push(("null root", Reply::data(json!({ root: Value::Null }))));
    }
    if kind == "create" {
        answers.push(("create without id", Reply::data(json!({ root: {"text": "new", "userId": null} }))));
    }
    answers
}

#[tokio::test(start_paused = true)]
async fn inv_op_05_an_answer_that_cannot_be_read_never_acknowledges_a_mutation() {
    let mut acknowledged = Vec::new();
    for kind in ["create", "update", "destroy", "action"] {
        let root = queue(kind).2;
        for (label, reply) in unreadable_answers(kind, root) {
            let (rig, operation, _) = queue(kind);
            rig.transport.enqueue(operation, reply);
            crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
            let open = rig.db.outbox().unwrap();
            let state = open.first().map(|entry| rig.db.sync_info("moments", &entry.operation.row_ids[0]).unwrap().state);
            if state != Some(SyncState::DeliveryUnknown) {
                acknowledged.push(format!("{kind} / {label}: {state:?}"));
            }
        }
    }
    assert!(acknowledged.is_empty(), "answers taken as acknowledgements: {acknowledged:#?}");
}

#[tokio::test(start_paused = true)]
async fn inv_op_14_an_answer_that_cannot_be_matched_is_kept_in_quarantine_and_the_operation_stays_open() {
    for kind in ["update", "destroy", "action"] {
        let (rig, operation, _) = queue(kind);
        rig.transport.enqueue(operation, Reply::data(json!({"wrong": {"id": "m1"}})));
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
        let kept: usize = quarantine_counts(&rig.db.shared.store.lock().connection).unwrap().values().sum();
        assert_eq!((kept, rig.db.outbox().unwrap().len()), (1, 1), "{kind}");
    }
}
