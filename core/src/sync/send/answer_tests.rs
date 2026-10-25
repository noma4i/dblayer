use serde_json::json;

use crate::identity::RowId;
use crate::transport::Reply;

use super::super::rig::*;

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn starred(extra: serde_json::Value) -> Reply {
    let mut payload = json!({"moment": {"id": "m1", "text": "starred", "userId": null}});
    payload.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    Reply::data(json!({"starMoment": payload}))
}

#[tokio::test(start_paused = true)]
async fn the_answer_of_an_action_carries_the_declared_fields_of_the_payload() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.act("moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", starred(json!({"successMessage": "Shared", "copies": 3, "undeclared": "x"})));
    let answer = rig.sync.answer("moments", acted.operation).await.unwrap();
    assert_eq!(answer, json!({"successMessage": "Shared", "copies": 3}));
    assert!(rig.sync.answer("moments", acted.operation).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn a_declared_field_the_server_left_out_or_typed_otherwise_reads_as_null() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.act("moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", starred(json!({"copies": "many"})));
    assert_eq!(rig.sync.answer("moments", acted.operation).await.unwrap(), json!({"successMessage": null, "copies": null}));
}

#[tokio::test(start_paused = true)]
async fn the_answer_of_an_action_refused_before_it_was_asked_for_is_the_refusal() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.act("moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    rig.sync.flush().await.unwrap();
    let error = rig.sync.answer("moments", acted.operation).await.unwrap_err();
    assert!(error.to_string().contains("not allowed"));
}

#[tokio::test(start_paused = true)]
async fn the_answer_of_an_action_is_kept_before_the_commit_that_closes_its_operation_is_announced() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.act("moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    let (sequence, inner) = (acted.operation, rig.sync.inner.clone());
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let probe = seen.clone();
    *rig.db.shared.store.after_commit.lock().unwrap() = Some(Box::new(move |result| {
        if result.operations.contains(&sequence) {
            probe.lock().unwrap().push(crate::sync::locked(&inner.outcomes).take("moments", sequence).is_ok());
        }
    }));
    rig.transport.enqueue("StarMoment", starred(json!({"successMessage": "Shared", "copies": 3})));
    rig.sync.flush().await.unwrap();
    assert!(seen.lock().unwrap().contains(&true));
}
