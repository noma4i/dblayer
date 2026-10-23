use std::time::Duration;

use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;
use crate::commit::OperationStatus;

async fn server_destroys(rig: &Rig, id: &str) -> crate::sync::subscribe::EventSubscription {
    use crate::sync::subscribe::{EventEffect, SubscriptionDef};
    use crate::transport::{EventSpec, Response, SubscriptionEvent};
    rig.sync.register(crate::sync::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    let def = SubscriptionDef::new(EventSpec::new("moment_gone"), "MomentChanged", "moments", EventEffect::Destroy);
    let subscription = rig.sync.subscribe(def).await.unwrap();
    rig.transport.emit("MomentChanged", SubscriptionEvent::Next(Response::data(json!({"momentChanged": {"id": id}}))));
    let_tasks_run().await;
    subscription
}

#[tokio::test(start_paused = true)]
async fn inv_cascade_05_a_server_delete_closes_the_open_operations_without_sending_or_rolling_back() {
    for state in ["pending", "failed", "delivery_unknown"] {
        let rig = rig();
        put(&rig, "moments", moment("m1", "old"));
        rig.db.update("moments", &rid("m1"), params(json!({"text": "edited"}))).unwrap();
        let seq = rig.db.outbox().unwrap()[0].operation.id.0;
        match state {
            "failed" => fail(&rig.db, seq, OperationStatus::Failed),
            "delivery_unknown" => {
                start(&rig.db, seq);
                fail(&rig.db, seq, OperationStatus::DeliveryUnknown);
            }
            _ => {}
        }
        let _subscription = server_destroys(&rig, "m1").await;
        assert_eq!(text_of(&rig, "moments", "m1"), None, "state {state}");
        assert!(rig.db.outbox().unwrap().is_empty(), "state {state}");
        rig.sync.flush().await.unwrap();
        assert!(names(&rig).iter().all(|name| name == "MomentChanged"), "state {state}: {:?}", names(&rig));
        assert_eq!(text_of(&rig, "moments", "m1"), None, "state {state}");
    }
}

#[tokio::test(start_paused = true)]
async fn inv_cascade_05_an_answer_to_a_sending_operation_after_the_server_delete_does_not_bring_the_row_back() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "edited"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", updated("m1", "edited").after(Duration::from_secs(5)));
    let delete = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        server_destroys(&rig, "m1").await
    };
    let (flushed, _subscription) = tokio::join!(rig.sync.flush(), delete);
    flushed.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}
