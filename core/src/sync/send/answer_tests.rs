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
    let acted = rig.db.act(&rig.db.admit().unwrap(), "moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", starred(json!({"successMessage": "Shared", "copies": 3, "undeclared": "x"})));
    let answer = rig.sync.answer(&rig.sync.inner.db.admit().unwrap(), "moments", acted.operation).await.unwrap();
    assert_eq!(answer, json!({"successMessage": "Shared", "copies": 3}));
    assert!(rig.sync.answer(&rig.sync.inner.db.admit().unwrap(), "moments", acted.operation).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn a_declared_field_the_server_left_out_or_typed_otherwise_reads_as_null() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.act(&rig.db.admit().unwrap(), "moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", starred(json!({"copies": "many"})));
    assert_eq!(rig.sync.answer(&rig.sync.inner.db.admit().unwrap(), "moments", acted.operation).await.unwrap(), json!({"successMessage": null, "copies": null}));
}

#[tokio::test(start_paused = true)]
async fn the_answer_of_an_action_refused_before_it_was_asked_for_is_the_refusal() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.act(&rig.db.admit().unwrap(), "moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    let error = rig.sync.answer(&rig.sync.inner.db.admit().unwrap(), "moments", acted.operation).await.unwrap_err();
    assert!(error.to_string().contains("not allowed"));
}

#[tokio::test(start_paused = true)]
async fn the_answer_of_an_action_is_kept_before_the_commit_that_closes_its_operation_is_announced() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.act(&rig.db.admit().unwrap(), "moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    let (sequence, inner) = (acted.operation, rig.sync.inner.clone());
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let probe = seen.clone();
    *rig.db.shared.store.after_commit.lock().unwrap() = Some(Box::new(move |result| {
        if result.operations.contains(&sequence) {
            probe.lock().unwrap().push(crate::sync::locked(&inner.outcomes).take("moments", sequence).is_ok());
        }
    }));
    rig.transport.enqueue("StarMoment", starred(json!({"successMessage": "Shared", "copies": 3})));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(seen.lock().unwrap().contains(&true));
}

#[tokio::test(start_paused = true)]
async fn an_action_reply_with_every_payload_field_null_is_a_delivered_answer() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.act(&rig.db.admit().unwrap(), "moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", Reply::data(json!({"starMoment": {"successMessage": null, "copies": null, "moment": null, "viewer": null}})));
    assert_eq!(rig.sync.answer(&rig.sync.inner.db.admit().unwrap(), "moments", acted.operation).await.unwrap(), json!({"successMessage": null, "copies": null}));
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.find("moments", &rid("m1")).unwrap().unwrap().get("text"), Some(&json!("t")));
}

#[tokio::test(start_paused = true)]
async fn an_all_null_reply_commits_an_action_an_update_and_parks_a_create() {
    let all_null = |root: &str| Reply::data(json!({ root: {"id": null, "text": null, "userId": null} }));
    for (kind, operation, root, open) in [("action", "LikeMoment", "likeMoment", 0), ("update", "UpdateMoment", "updateMoment", 0), ("create", "CreateMoment", "createMoment", 1)] {
        let rig = rig();
        put(&rig, "moments", moment("m1", "t"));
        match kind {
            "action" => drop(rig.db.act(&rig.db.admit().unwrap(), "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap()),
            "update" => drop(rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "new"}))).unwrap()),
            _ => drop(rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "new"}))).unwrap()),
        }
        rig.transport.enqueue(operation, all_null(root));
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
        assert_eq!(rig.db.outbox().unwrap().len(), open, "{kind}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_create_whose_payload_fields_are_null_stays_delivery_unknown() {
    let rig = rig();
    let created = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"text": null}})));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.db.sync_info("moments", &created.id).unwrap().state, crate::syncstate::SyncState::DeliveryUnknown);
}

#[tokio::test(start_paused = true)]
async fn an_action_reply_without_a_payload_object_keeps_the_delivery_unknown() {
    for reply in [json!({"starMoment": null}), json!({"other": {"moment": null}}), json!({"starMoment": "garbage"})] {
        let rig = rig();
        put(&rig, "moments", moment("m1", "t"));
        let acted = rig.db.act(&rig.db.admit().unwrap(), "moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
        rig.transport.enqueue("StarMoment", Reply::data(reply.clone()));
        let error = rig.sync.answer(&rig.sync.inner.db.admit().unwrap(), "moments", acted.operation).await.unwrap_err();
        assert!(error.to_string().contains("the answer is unknown"), "{reply}: {error}");
        assert_eq!(rig.db.outbox().unwrap().len(), 1, "{reply}");
    }
}
