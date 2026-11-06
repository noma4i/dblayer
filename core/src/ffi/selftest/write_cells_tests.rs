//! Cells of the owner Outbox seen from the public API: the checks of a call before anything is written, `send` and `set_local`.

use std::sync::Arc;

use serde_json::json;

use super::bridge_tests::message;
use super::flow_fixtures::{add_chat, add_media, chat_row, land_event};
use super::rig_tests::{bound, Rig, World};
use super::upload_fixtures::add_message_file;
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::tape::Rows;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::{DbError, WireOperationOutcome};
use crate::test_wait::yield_until;
use crate::upload::UploadFile;

const THREAD: &str = r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#;

async fn rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| { add_chat(models); add_media(models); add_message_file(models); }).await;
    land_event(&rig, "Chat", chat_row("c1", 0));
    rig
}

async fn thread(rig: &Rig) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(THREAD, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    rows
}

fn command(model: &str, values: serde_json::Value, epoch: u64, upload: Option<CommandUpload>) -> CommandRequest {
    CommandRequest { epoch, model: model.into(), values_json: values.to_string(), args_json: "{}".into(), upload }
}

fn is_invalid(error: &DbError) -> bool { matches!(error, DbError::Invalid { .. }) }

/// `(нет) x Submit` with a value that does not fit the nested type: `Err(Invalid{path})`, no optimistic row, no intent.
#[tokio::test(flavor = "multi_thread")]
async fn out_submit_invalid_value() {
    let rig = rig().await;
    let rows = thread(&rig).await;
    let epoch = rig.database.session_epoch();
    let bad = command("Message", json!({"chatId": "c1", "text": "x", "at": 1, "media": {"bogus": 1}}), epoch, None);
    let error = bound(rig.database.send(bad)).await.unwrap_err();
    assert!(matches!(&error, DbError::Invalid { detail } if detail.contains("media.bogus")), "{error:?}");
    rig.settle().await;
    assert_eq!((rows.len(), rig.world.count("SendMessage")), (0, 0), "nothing is shown and nothing is sent");
    rig.database.close_now();
}

/// A `send` without a file for a model that declares no durable `create` is refused before anything is written.
#[tokio::test(flavor = "multi_thread")]
async fn out_send_not_durable() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    let error = bound(rig.database.send(command("Chat", json!({"name": "n"}), epoch, None))).await.unwrap_err();
    assert!(is_invalid(&error), "{error:?}");
    rig.database.close_now();
}

/// A `send` with a file whose action is not declared `upload: true` is refused.
#[tokio::test(flavor = "multi_thread")]
async fn out_send_unknown_upload_action() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    let file = UploadFile::new("/nonexistent");
    let upload = CommandUpload { action: "create".into(), request: WireUploadRequest::of(&file) };
    let error = bound(rig.database.send(command("Message", json!({"chatId": "c1", "text": "x", "at": 1}), epoch, Some(upload)))).await.unwrap_err();
    assert!(is_invalid(&error), "{error:?}");
    rig.database.close_now();
}

/// A durable `send` without the network: the receipt comes after the commit, the row stands, the request leaves when the network returns.
#[tokio::test(flavor = "multi_thread")]
async fn out_send_durable() {
    let rig = rig().await;
    let rows = thread(&rig).await;
    rig.database.set_online(false).unwrap();
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.send(message(epoch, "later", 5))).await.unwrap();
    yield_until("the row stands", || rows.len() == 1).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "no network, no request");
    rig.database.set_online(true).unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    assert_eq!(rig.world.count("SendMessage"), 1);
    rig.database.close_now();
}

/// `set_local` on a path that is not declared local: `Err(Invalid{path})`.
#[tokio::test(flavor = "multi_thread")]
async fn out_local_set_local_not_local() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    land_event(&rig, "Message", json!({"id": "m1", "chatId": "c1", "text": "t", "at": 1}));
    let error = bound(rig.database.set_local(epoch, "Message".into(), "m1".into(), "text".into(), json!("x").to_string())).await.unwrap_err();
    assert!(is_invalid(&error), "{error:?}");
    rig.database.close_now();
}

/// `set_local` with the stored value writes nothing and tells nobody.
#[tokio::test(flavor = "multi_thread")]
async fn out_local_set_local_same() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    land_event(&rig, "Message", json!({"id": "m1", "chatId": "c1", "text": "t", "at": 1}));
    let rows = thread(&rig).await;
    yield_until("the row stands", || rows.len() == 1).await;
    bound(rig.database.set_local(epoch, "Message".into(), "m1".into(), "localPreviewUrl".into(), json!("file:///a").to_string())).await.unwrap();
    yield_until("the value shows", || rows.1.lock().unwrap().len() >= 2).await;
    let before = (rows.1.lock().unwrap().len(), rig.database.db.revision_seq().unwrap());
    bound(rig.database.set_local(epoch, "Message".into(), "m1".into(), "localPreviewUrl".into(), json!("file:///a").to_string())).await.unwrap();
    rig.settle().await;
    assert_eq!((rows.1.lock().unwrap().len(), rig.database.db.revision_seq().unwrap()), before, "no commit, no delivery");
    rig.database.close_now();
}
