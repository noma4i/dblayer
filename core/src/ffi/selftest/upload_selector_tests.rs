use serde_json::json;

use super::flow_fixtures::{add_chat, chat_row, land_owned};
use super::rig_tests::{bound, Rig, World};
use super::upload_fixtures::add_uploads;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::{WireOperationOutcome, WireOperationReceipt};
use crate::owners::write::upload::UploadFile;
use crate::test_wait::settled;

/// boundary_selector_without_action (D-173): `discard` without an action ends the first open operation of the row by `op_id`, whether it is a mutation or a transfer, and the other operation still goes out.
#[tokio::test(flavor = "multi_thread")]
async fn a_discard_without_action_ends_the_first_operation_of_the_row() {
    for transfer_first in [false, true] {
        let rig = Rig::signed_as(World::new("server", "A", true), false, |models| {
            add_chat(models);
            add_uploads(models);
        }).await;
        land_owned(&rig, "Chat", chat_row("c1", 0)).await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("logo.jpg");
        std::fs::write(&path, b"jpeg").unwrap();
        let epoch = rig.database.session_epoch();
        rig.database.set_online(false).unwrap();

        let upload = || async { bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), WireUploadRequest::of(&{ let mut file = UploadFile::new(path.to_str().unwrap()); file.variables = json!({"id": "c1"}).as_object().unwrap().clone(); file }))).await.unwrap() };
        let pin = || async { bound(rig.database.act(epoch, "Chat".into(), "c1".into(), "pin".into(), "{}".into(), json!({"pinned": true}).to_string())).await.unwrap() };
        let (transfer, mutation): (WireOperationReceipt, WireOperationReceipt) = if transfer_first {
            let transfer = upload().await;
            (transfer, pin().await)
        } else {
            let mutation = pin().await;
            (upload().await, mutation)
        };

        bound(rig.database.discard(epoch, "Chat".into(), "c1".into(), None)).await.unwrap();
        let (ended, kept) = if transfer_first { (transfer, mutation) } else { (mutation, transfer) };
        assert!(matches!(bound(rig.database.outcome(ended)).await.unwrap(), WireOperationOutcome::Interrupted), "transfer_first {transfer_first}: the first operation ended");

        rig.database.set_online(true).unwrap();
        assert!(matches!(bound(rig.database.outcome(kept)).await.unwrap(), WireOperationOutcome::Confirmed { .. }), "transfer_first {transfer_first}: the other operation went out");
        settled(&rig, "the server saw the kept operation only", || rig.world.count("ChatPin") + rig.world.count("ChatUpdatePhoto") == 1).await;
        assert_eq!(rig.world.count(if transfer_first { "ChatPin" } else { "ChatUpdatePhoto" }), 1, "transfer_first {transfer_first}");
        rig.database.close_now();
    }
}
