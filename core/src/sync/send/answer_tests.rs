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
async fn the_answer_of_an_action_carries_its_declared_fields_and_null_for_a_missing_or_mistyped_one() {
    for (reply, answer) in [
        (starred(json!({"successMessage": "Shared", "copies": 3, "undeclared": "x"})), json!({"successMessage": "Shared", "copies": 3})),
        (starred(json!({"copies": "many"})), json!({"successMessage": null, "copies": null})),
        (Reply::data(json!({"starMoment": {"successMessage": null, "copies": null, "moment": null, "viewer": null}})), json!({"successMessage": null, "copies": null})),
    ] {
        let rig = rig();
        put(&rig, "moments", moment("m1", "t"));
        let (_, ticket) = act_claimed(&rig.sync, "moments", "star", &rid("m1"), params(json!({})), params(json!({})));
        rig.transport.enqueue("StarMoment", reply);
        flushed(&rig).await;
        assert_eq!(settled(&ticket), super::SendResult::Committed, "{answer}");
        assert_eq!(ticket.finished().unwrap().answer, Some(answer.clone()));
        assert!(rig.db.outbox().unwrap().is_empty(), "{answer}");
    }
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
        let (_, ticket) = act_claimed(&rig.sync, "moments", "star", &rid("m1"), params(json!({})), params(json!({})));
        rig.transport.enqueue("StarMoment", Reply::data(reply.clone()));
        flushed(&rig).await;
        assert_eq!(settled(&ticket), super::SendResult::Quarantined, "{reply}");
        assert_eq!(rig.db.outbox().unwrap().len(), 1, "{reply}");
    }
}
