use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, add_profile, cell, int, text};
use super::rig_events::events;
use super::rig_tests::{bound, Fault, Rig};
use crate::ffi::{DbError, WireOperationReceipt};

async fn rig() -> Rig {
    let rig = Rig::encrypted_edited(false, false, false, events(|models: &mut Value| {
        add_chat(models);
        add_profile(models);
        let user = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "User").unwrap();
        let action = user["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "update").unwrap();
        action["on_failure"] = json!("keep");
    })).await;
    rig.login_ios("A").await;
    rig
}

async fn update(rig: &Rig, values: Value, name: &str) -> WireOperationReceipt {
    bound(rig.database.act(rig.database.session_epoch(), "User".into(), "user".into(), "update".into(), values.to_string(), json!({"name": name}).to_string())).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn out_discard_two_ops_same_key_failed_only() {
    for failed_second in [false, true] {
        let rig = rig().await;
        let epoch = rig.database.session_epoch();
        if failed_second {
            let first = update(&rig, json!({"name":"first"}), "first").await;
            assert!(bound(rig.database.confirmed(first)).await.is_ok());
        }
        let refused = rig.world.hold_then("ProfileUpdate", Fault::Http(422));
        let failed = update(&rig, json!({"name":"failed"}), "failed").await;
        refused.entered().await;
        refused.release();
        assert!(matches!(bound(rig.database.confirmed(failed.clone())).await, Err(DbError::Rejected { kept: true, .. })));
        rig.database.set_online(false).unwrap();
        let waiting = if failed_second { None } else { Some(update(&rig, json!({"secondaryChatsCount":7}), "waiting").await) };
        let result = bound(rig.database.discard(epoch, "User".into(), "user".into(), Some("update".into()))).await;
        assert!(result.is_ok(), "failed_second={failed_second}: {result:?}");
        let row = bound(rig.database.read_row(epoch, "User".into(), "user".into())).await.unwrap().unwrap();
        assert_eq!(cell(&rig, "User", &row, "name"), text(if failed_second { "first" } else { "A" }));
        if let Some(waiting) = waiting {
            assert_eq!(cell(&rig, "User", &row, "secondaryChatsCount"), int(7));
            rig.database.set_online(true).unwrap();
            assert!(bound(rig.database.confirmed(waiting)).await.is_ok());
        }
        rig.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn out_retry_two_ops_same_key_failed_only() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    let refused = rig.world.hold_then("ProfileUpdate", Fault::Http(422));
    let failed = update(&rig, json!({"name":"failed"}), "failed").await;
    refused.entered().await;
    refused.release();
    assert!(matches!(bound(rig.database.confirmed(failed)).await, Err(DbError::Rejected { kept: true, .. })));
    rig.database.set_online(false).unwrap();
    let waiting = update(&rig, json!({"secondaryChatsCount":7}), "waiting").await;
    let retried = bound(rig.database.retry(epoch, "User".into(), "user".into(), Some("update".into()), None)).await.unwrap();
    rig.database.set_online(true).unwrap();
    assert!(bound(rig.database.confirmed(retried)).await.is_ok());
    assert!(bound(rig.database.confirmed(waiting)).await.is_ok());
    assert_eq!(rig.world.count("ProfileUpdate"), 3, "one refusal and one request per operation");
    rig.database.close_now();
}
