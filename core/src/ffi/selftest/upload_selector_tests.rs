use serde_json::json;

use super::flow_fixtures::{add_chat, chat_row};
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use super::upload_fixtures::add_uploads;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::{DbError, WireOperationOutcome, WireOperationReceipt};
use crate::owners::write::upload::UploadFile;
use crate::test_wait::settled;

/// boundary_selector_without_action (D-173): `discard` without an action ends the first open operation of the row by `op_id`, whether it is a mutation or a transfer, and the other operation still goes out.
#[tokio::test(flavor = "multi_thread")]
async fn boundary_selector_without_action() {
    for transfer_first in [false, true] {
        let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| {
            add_chat(models);
            add_uploads(models);
        })).await;
        rig.land("Chat", chat_row("c1", 0)).await;
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
        assert!(matches!(bound(rig.database.outcome(ended)).await.unwrap(), WireOperationOutcome::Ended { cause: crate::ffi::EndCause::Discarded }), "transfer_first {transfer_first}: the first operation ended");

        rig.database.set_online(true).unwrap();
        assert!(bound(rig.database.confirmed(kept, false)).await.is_ok(), "transfer_first {transfer_first}: the other operation went out");
        settled(&rig, "the server saw the kept operation only", || rig.world.count("ChatPin") + rig.world.count("ChatUpdatePhoto") == 1).await;
        assert_eq!(rig.world.count(if transfer_first { "ChatPin" } else { "ChatUpdatePhoto" }), 1, "transfer_first {transfer_first}");
        rig.database.close_now();
    }
}

/// boundary_transfer_selector (P-BND-05, xfer_accept_check_order): `retry` and `discard` with an undeclared action end `Invalid` before the row is read, a declared action with no transfer on the row ends `NotFound`, in every order, and the upload of the row, which has another action, goes out untouched.
#[tokio::test(flavor = "multi_thread")]
async fn boundary_transfer_selector() {
    for discard_first in [false, true] {
        let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| {
            add_chat(models);
            add_uploads(models);
        })).await;
        rig.land("Chat", chat_row("c1", 0)).await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("logo.jpg");
        std::fs::write(&path, b"jpeg").unwrap();
        let epoch = rig.database.session_epoch();
        rig.database.set_online(false).unwrap();
        let mut file = UploadFile::new(path.to_str().unwrap());
        file.variables = json!({"id": "c1"}).as_object().unwrap().clone();
        let upload = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), WireUploadRequest::of(&file))).await.unwrap();

        let discard = || async { bound(rig.database.discard(epoch, "Chat".into(), "c1".into(), Some("no_such_action".into()))).await };
        let retry = || async { bound(rig.database.retry(epoch, "Chat".into(), "c1".into(), Some("no_such_action".into()), None)).await.map(drop) };
        let (first, second) = if discard_first { (discard().await, retry().await) } else { (retry().await, discard().await) };
        for refused in [first, second] {
            assert!(matches!(refused, Err(DbError::Invalid { .. })), "an undeclared action is Invalid before NotFound, discard_first {discard_first}: {refused:?}");
        }

        let declared_discard = || async { bound(rig.database.discard(epoch, "Chat".into(), "c2".into(), Some("update_photo".into()))).await };
        let declared_retry = || async { bound(rig.database.retry(epoch, "Chat".into(), "c2".into(), Some("update_photo".into()), None)).await.map(drop) };
        let (first, second) = if discard_first { (declared_discard().await, declared_retry().await) } else { (declared_retry().await, declared_discard().await) };
        for refused in [first, second] {
            assert!(matches!(refused, Err(DbError::NotFound { .. })), "a declared action with no transfer on the row is NotFound, discard_first {discard_first}: {refused:?}");
        }

        rig.database.set_online(true).unwrap();
        assert!(bound(rig.database.confirmed(upload, false)).await.is_ok(), "discard_first {discard_first}: the upload of the other action went out");
        settled(&rig, "the server saw the upload once", || rig.world.count("ChatUpdatePhoto") == 1).await;
        rig.database.close_now();
    }
}
