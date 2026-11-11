//! Cells of the owner Outbox seen from the public API: the checks of a call before anything is written, `send` and `set_local`.

use std::sync::Arc;

use serde_json::json;

use super::flow_fixtures::{add_chat, add_media, cell, chat_row, int, text};
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use super::upload_fixtures::add_message_file;
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::DbError;
use crate::test_wait::settled;
use crate::owners::write::upload::UploadFile;

const THREAD: &str = r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#;

async fn rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| { add_chat(models); add_media(models); add_message_file(models); })).await;
    rig.land("Chat", chat_row("c1", 0)).await;
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

/// `(none) x Submit` with a value that does not fit the nested type: `Err(Invalid{path})`, no optimistic row, no intent.
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

/// A `send` without a file for a model that declares no `operation :create` is refused before anything is written.
#[tokio::test(flavor = "multi_thread")]
async fn out_send_undeclared() {
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

/// `set_local` on a path that is not declared local: `Err(Invalid{path})`.
#[tokio::test(flavor = "multi_thread")]
async fn out_local_set_local_not_local() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "t", "at": 1})).await;
    let error = bound(rig.database.set_local(epoch, "Message".into(), "m1".into(), "text".into(), json!("x").to_string())).await.unwrap_err();
    assert!(is_invalid(&error), "{error:?}");
    rig.database.close_now();
}

/// `set_local` with the stored value writes nothing and tells nobody.
#[tokio::test(flavor = "multi_thread")]
async fn out_local_set_local_same() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "t", "at": 1})).await;
    let rows = thread(&rig).await;
    settled(&rig, "the row stands", || rows.len() == 1).await;
    bound(rig.database.set_local(epoch, "Message".into(), "m1".into(), "localPreviewUrl".into(), json!("file:///a").to_string())).await.unwrap();
    settled(&rig, "the value shows", || rows.1.lock().unwrap().len() >= 2).await;
    let before = (rows.1.lock().unwrap().len(), rig.database.db.revision_seq().unwrap());
    bound(rig.database.set_local(epoch, "Message".into(), "m1".into(), "localPreviewUrl".into(), json!("file:///a").to_string())).await.unwrap();
    rig.settle().await;
    assert_eq!((rows.1.lock().unwrap().len(), rig.database.db.revision_seq().unwrap()), before, "no commit, no delivery");
    rig.database.close_now();
}

fn missing(error: &DbError, model: &str, row: &str) -> bool { matches!(error, DbError::NotFound { detail } if detail.contains(model) && detail.contains(row)) }

/// P-OUT-40 out_submit_missing_row_not_found: `update`, `act` and `destroy` on an absent row, or on a row that a pending `destroy` hides, end `NotFound` and send nothing.
#[tokio::test(flavor = "multi_thread")]
async fn out_submit_missing_row_not_found() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    let before = rig.world.hits();
    let update = bound(rig.database.update(epoch, "Message".into(), "gone".into(), r#"{"text":"x"}"#.into(), "{}".into())).await.unwrap_err();
    let act = bound(rig.database.act(epoch, "Chat".into(), "gone".into(), "mark_read".into(), "{}".into(), "{}".into())).await.unwrap_err();
    let destroy = bound(rig.database.destroy(epoch, "Message".into(), "gone".into())).await.unwrap_err();
    assert!(missing(&update, "Message", "gone") && missing(&act, "Chat", "gone") && missing(&destroy, "Message", "gone"), "{update:?} {act:?} {destroy:?}");

    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "t", "at": 1})).await;
    let hold = rig.world.hold_next("MessageDelete");
    bound(rig.database.destroy(epoch, "Message".into(), "m1".into())).await.unwrap();
    hold.entered().await;
    let sent = rig.world.hits();
    let hidden = bound(rig.database.update(epoch, "Message".into(), "m1".into(), r#"{"text":"x"}"#.into(), "{}".into())).await.unwrap_err();
    let hidden_act = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "update".into(), r#"{"text":"x"}"#.into(), r#"{"text":"x"}"#.into())).await.unwrap_err();
    assert!(missing(&hidden, "Message", "m1") && missing(&hidden_act, "Message", "m1"), "{hidden:?} {hidden_act:?}");
    hold.release();
    rig.settle().await;
    assert_eq!((rig.world.hits() - sent, sent - before), (0, 1), "only the held MessageDelete went out");
    rig.database.close_now();
}

/// P-OUT-41 out_create_existing_id_already_exists: `create` with the id of a stored row or of a row that a pending `destroy` hides ends `AlreadyExists` and sends nothing; a stale epoch is refused first.
#[tokio::test(flavor = "multi_thread")]
async fn out_create_existing_id_already_exists() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "t", "at": 1})).await;
    rig.land("Message", json!({"id": "m2", "chatId": "c1", "text": "t", "at": 2})).await;
    let again = |id: &str, epoch: u64| command("Message", json!({"id": id, "chatId": "c1", "text": "other", "at": 3}), epoch, None);
    let stored = bound(rig.database.send(again("m1", epoch))).await.unwrap_err();
    let stale = bound(rig.database.send(again("m1", epoch + 1))).await.unwrap_err();
    assert!(matches!(&stored, DbError::AlreadyExists { model, id } if model == "Message" && id == "m1"), "{stored:?}");
    assert!(matches!(&stale, DbError::NoSession { .. }), "{stale:?}");

    let hold = rig.world.hold_next("MessageDelete");
    bound(rig.database.destroy(epoch, "Message".into(), "m2".into())).await.unwrap();
    hold.entered().await;
    let sent = rig.world.hits();
    let open = bound(rig.database.send(again("m2", epoch))).await.unwrap_err();
    assert!(matches!(&open, DbError::AlreadyExists { model, id } if model == "Message" && id == "m2"), "{open:?}");
    hold.release();
    rig.settle().await;
    assert_eq!(rig.world.hits(), sent, "nothing is sent");
    rig.database.close_now();
}

fn extra(models: &mut serde_json::Value, fields: Vec<crate::schema::FieldSpec>) {
    let message = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Message").unwrap();
    for field in fields { message["fields"].as_array_mut().unwrap().push(serde_json::to_value(field).unwrap()); }
}

/// P-OUT-42 out_create_fills_zero_values: `create` fills a required field without a value with the zero of its type; nullable, optional and defaulted fields and `id` stay as they are.
#[tokio::test(flavor = "multi_thread")]
async fn out_create_fills_zero_values() {
    use crate::schema::FieldSpec;
    use crate::value::ScalarKind;
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| {
        add_chat(models);
        extra(models, vec![
            FieldSpec::scalar("flag", ScalarKind::Bool),
            FieldSpec::scalar("sentOn", ScalarKind::Date),
            FieldSpec::enumeration("kind", &["draft", "sent"]),
            FieldSpec::object("meta"),
            FieldSpec::array("tags"),
            FieldSpec::scalar("note", ScalarKind::Str).nullable(),
            FieldSpec::scalar("memo", ScalarKind::Str).optional(),
            FieldSpec::scalar("rank", ScalarKind::Int).with_default(json!(7)),
        ]);
    })).await;
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.send(command("Message", json!({"chatId": "c1"}), epoch, None))).await.unwrap();
    let row = receipt.optimistic_value.unwrap();
    let at = |field: &str| cell(&rig, "Message", &row, field);
    assert_eq!((at("text"), at("at"), at("flag")), (text(""), int(0), Cell::Bool { value: false }));
    assert_eq!(at("kind"), text("draft"));
    assert_eq!((at("meta"), at("tags")), (Cell::Keyed { entries: Vec::new() }, Cell::Seq { items: Vec::new() }));
    assert_eq!(at("sentOn"), text("1970-01-01T00:16:40.000Z"));
    assert_eq!((at("note"), at("memo")), (Cell::Null, Cell::Null));
    assert_eq!(at("rank"), int(7));
    assert_ne!(row.id, "");
    rig.database.close_now();
}

/// P-OUT-43 out_local_receipt_without_operation: a write of a `storage: local` model and an `act` without `mutation` answer a receipt without an operation, with the revision of the batch and the row of the batch; nothing is sent.
#[tokio::test(flavor = "multi_thread")]
async fn out_local_receipt_without_operation() {
    use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, RootShape, Storage};
    use crate::value::ScalarKind;
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models: &mut serde_json::Value| {
        events(add_chat)(models);
        let mut note = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
        note.storage = Storage::Local;
        let models = models.as_array_mut().unwrap();
        models.push(serde_json::to_value(note).unwrap());
        models.iter_mut().find(|model| model["key"] == "Chat").unwrap()["actions"].as_array_mut().unwrap().push(serde_json::to_value(ActionDeclaration::new("retitle", RootShape::Upsert)).unwrap());
    }).await;
    let epoch = rig.database.session_epoch();

    let made = bound(rig.database.create(epoch, "Note".into(), r#"{"text":"a"}"#.into(), "{}".into())).await.unwrap();
    let made_row = made.optimistic_value.clone().unwrap();
    assert_eq!((made.operation, made.revision), (None, rig.database.db.revision_seq().unwrap()));
    assert_eq!(cell(&rig, "Note", &made_row, "text"), text("a"));
    let before = rig.world.hits();

    rig.land("Chat", chat_row("c1", 0)).await;
    let acted = bound(rig.database.act(epoch, "Chat".into(), "c1".into(), "retitle".into(), r#"{"name":"b"}"#.into(), "{}".into())).await.unwrap();
    let acted_row = acted.optimistic_value.clone().unwrap();
    assert_eq!((acted.operation, acted.revision), (None, rig.database.db.revision_seq().unwrap()));
    assert_eq!((acted_row.id.clone(), cell(&rig, "Chat", &acted_row, "name"), acted_row.revision), ("c1".to_string(), text("b"), acted.revision));
    assert!(acted.revision > made.revision);
    rig.settle().await;
    assert_eq!(rig.world.hits(), before, "nothing is sent");
    rig.database.close_now();
}

/// P-OUT-53 out_act_without_optimistic_sends: an `act` that declares no optimistic effect leaves the row as it was and the mutation goes out.
#[tokio::test(flavor = "multi_thread")]
async fn out_act_without_optimistic_sends() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    let mut chat = chat_row("c1", 0);
    chat["unreadCount"] = json!(5);
    rig.land("Chat", chat).await;
    let hold = rig.world.hold_next("ChatMarkRead");

    let receipt = bound(rig.database.act(epoch, "Chat".into(), "c1".into(), "mark_read".into(), "{}".into(), "{}".into())).await.unwrap();
    hold.entered().await;

    assert_eq!(rig.world.count("ChatMarkRead"), 1, "the mutation goes out");
    let row = bound(rig.database.read_row(epoch, "Chat".into(), "c1".into())).await.unwrap().unwrap();
    assert_eq!(cell(&rig, "Chat", &row, "unreadCount"), int(5), "the row is not changed before the answer");
    assert_eq!(cell(&rig, "Chat", receipt.optimistic_value.as_ref().unwrap(), "unreadCount"), int(5));
    hold.release();
    assert!(bound(rig.database.confirmed(receipt, false)).await.is_ok());
    rig.database.close_now();
}

/// P-OUT-44 out_confirmed_checks_epoch_end: `confirmed` refuses a changed receipt before it looks at the end of the epoch, answers the cause when the epoch ended, and `Closed` after the close.
#[tokio::test(flavor = "multi_thread")]
async fn out_confirmed_checks_epoch_end() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("SendMessage");
    let receipt = bound(rig.database.send(command("Message", json!({"chatId": "c1", "text": "x", "at": 1}), epoch, None))).await.unwrap();
    hold.entered().await;
    let mut changed = receipt.clone();
    changed.revision += 1;
    assert!(matches!(bound(rig.database.confirmed(changed.clone(), false)).await, Err(DbError::Invalid { .. })));

    rig.database.session_reset().await.unwrap();
    let ended = bound(rig.database.confirmed(receipt.clone(), false)).await.unwrap_err();
    let ended_changed = bound(rig.database.confirmed(changed, false)).await.unwrap_err();
    assert!(matches!(ended, DbError::Interrupted { .. } | DbError::NoSession { .. }), "{ended:?}");
    assert!(matches!(ended_changed, DbError::Invalid { .. }), "{ended_changed:?}");

    hold.release();
    rig.database.close_now();
    let closed = bound(rig.database.confirmed(receipt, false)).await.unwrap_err();
    assert!(matches!(closed, DbError::Closed { .. }), "{closed:?}");
}
