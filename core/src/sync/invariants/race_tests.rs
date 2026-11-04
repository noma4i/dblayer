use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;
use super::spec_fixtures::mentions;

#[tokio::test(start_paused = true)]
async fn inv_id_05_inv_op_18_an_update_queued_while_the_create_is_confirmed_follows_the_real_id_or_fails() {
    for schedule in ["before the flush", "while the create is in flight", "after the confirmation"] {
        let rig = rig();
        let local = rig.db.as_app().create("moments", params(json!({"text": "hello"}))).unwrap();
        let temp = local.id.to_string();
        let edit = {
            let (db, id) = (rig.db.clone(), local.id.clone());
            move || db.as_app().update("moments", &id, params(json!({"text": "edited"}))).is_ok()
        };
        rig.transport.enqueue("CreateMoment", created("m77", "hello"));
        let queued = match schedule {
            "before the flush" => {
                let queued = edit();
                crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
                queued
            }
            "while the create is in flight" => {
                let queued = Arc::new(AtomicBool::new(false));
                let (flag, once) = (queued.clone(), AtomicBool::new(false));
                rig.transport.respond_with(move |request| {
                    if request.operation_name == "CreateMoment" && !once.swap(true, Ordering::SeqCst) {
                        flag.store(edit(), Ordering::SeqCst);
                    }
                    None
                });
                crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
                queued.load(Ordering::SeqCst)
            }
            _ => {
                crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
                edit()
            }
        };
        assert!(queued || schedule == "after the confirmation", "{schedule}: the update must be accepted while the create is open");
        assert_eq!(mentions(&rig.db, &temp), Vec::<String>::new(), "{schedule}, queued {queued}");
        let sent_with_temp = rig.transport.calls().iter().filter(|call| call.variables.to_string().contains("temp-")).count();
        assert_eq!(sent_with_temp, 0, "{schedule}: calls carried the temp id");
        assert!(rig.db.outbox().unwrap().iter().all(|entry| entry.operation.row_ids.iter().all(|id| id.as_str() == "m77")), "{schedule}");
    }
}
