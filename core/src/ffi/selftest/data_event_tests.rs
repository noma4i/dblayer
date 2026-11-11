use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, add_moment, cell, chat_row, int, text};
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::wire::WireRow;
use crate::ffi::ReadValue;
use crate::test_wait::settled;
use crate::schema::FieldSpec;

/// P-DATA-93: an event with the same values changes neither the row nor its revision and wakes no row stream, yet `Events` gets a new `Event` at the next position.
#[tokio::test(flavor = "multi_thread")]
async fn data_event_same_values_new_event() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|_| {})).await;
    let row = json!({"id": "u1", "name": "Ann"});
    let first = rig.land_seen("User", row.clone()).await;
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"User","relation":"all","params":{}}"#, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    let stored = |rig: &Rig| rig.database.db.read(&["User"], |reader| reader.stored_row("User", "u1")).unwrap().map(|stored| (stored.revision, Value::Object(stored.row)));
    let (delivered, before) = (rows.1.lock().unwrap().len(), stored(&rig));
    let second = rig.land_seen("User", row).await;
    assert_eq!(stored(&rig), before, "the row and its revision stay");
    assert_eq!(rows.1.lock().unwrap().len(), delivered, "the row stream gets nothing");
    let (ReadValue::Event { position: first, .. }, ReadValue::Event { position: second, .. }) = (first, second) else { unreachable!() };
    assert_eq!(second, first + 1, "Events gets the event at the next position");
    rig.database.close_now();
}

/// P-XFER-46: `attach`, `detach`, `file` and `url` take the temporary id of a created row to its real id and ask the network nothing; `url` is the local path of the file.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_file_calls_resolve_real_id() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| {
        add_moment(models);
        let moment = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Moment").unwrap();
        moment["fields"].as_array_mut().unwrap().push(serde_json::to_value(FieldSpec::file("photo").nullable()).unwrap());
    })).await;
    rig.world.answer_with("MomentCreate", json!({"data":{"momentCreate":{"__typename":"Moment","id":"m9","userId":"u1","text":"new"}}}));
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.create(epoch, "Moment".into(), json!({"userId": "u1", "text": "new"}).to_string(), "{}".into())).await.unwrap();
    let temporary = receipt.optimistic_value.clone().unwrap().id;
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    assert_ne!(temporary, "m9");
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::write(&source, [1, 2, 3]).unwrap();
    let hits = rig.world.hits();
    let attached = bound(rig.database.attach_file(epoch, "Moment".into(), temporary.clone(), "photo".into(), source.to_string_lossy().into(), "p.png".into(), "image/png".into())).await.unwrap();
    assert_eq!(attached.size, 3);
    let real = rig.database.db.file("Moment", &crate::identity::RowId::new("m9").unwrap(), "photo").unwrap();
    assert!(real.is_some(), "the file stands on the real row");
    assert_eq!(bound(rig.database.file(epoch, "Moment".into(), temporary.clone(), "photo".into())).await.unwrap().and_then(|file| file.name), Some("p.png".to_string()));
    let url = bound(rig.database.url(epoch, "Moment".into(), temporary.clone(), "photo".into())).await.unwrap().unwrap();
    assert_eq!(std::fs::read(&url).unwrap(), [1, 2, 3], "url is the local path of the file");
    assert!(bound(rig.database.detach(epoch, "Moment".into(), temporary, "photo".into())).await.unwrap());
    assert!(rig.database.db.file("Moment", &crate::identity::RowId::new("m9").unwrap(), "photo").unwrap().is_none(), "the file left the real row");
    assert_eq!(rig.world.hits(), hits, "the file calls asked the network nothing");
    rig.database.close_now();
}

const THREAD: &str = r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#;

async fn thread_rig() -> (Rig, Arc<Rows>) {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(add_chat)).await;
    rig.land("Chat", chat_row("c1", 0)).await;
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(THREAD, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    (rig, rows)
}

fn row_of(rows: &Rows, id: &str) -> Option<WireRow> { rows.0.lock().unwrap().iter().find(|row| row.id == id).cloned() }

/// P-DATA-94: an event of a model without `server_revision` applies whatever its values, older or newer than the stored ones.
#[tokio::test(flavor = "multi_thread")]
async fn data_event_without_revision_applies() {
    let (rig, rows) = thread_rig().await;
    assert!(rig.database.db.spec("Message").unwrap().server_revision.is_none(), "the model declares no server_revision");
    let cells = |rig: &Rig, id: &str| row_of(&rows, id).map(|row| (cell(rig, "Message", &row, "text"), cell(rig, "Message", &row, "at")));
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "first", "at": 9})).await;
    assert_eq!(cells(&rig, "m1"), Some((text("first"), int(9))), "the first event applies");
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "second", "at": 1})).await;
    assert_eq!(cells(&rig, "m1"), Some((text("second"), int(1))), "the second event with lower values applies over the first");
    rig.database.close_now();
}

/// P-DATA-95: the delete event of a row that is not stored gives `Events` an `Event` with no `before` and no `after`, and leaves a tombstone: a page of a window that was asked before the event and brings the row does not bring it back.
#[tokio::test(flavor = "multi_thread")]
async fn data_event_delete_missing_row() {
    let (rig, rows) = thread_rig().await;
    rig.world.seed_message("A", "m404", "late", 1);
    let epoch = rig.database.session_epoch();
    let gate = rig.world.hold_next("Thread");
    let database = rig.database.clone();
    let load = tokio::spawn(async move { database.load(epoch, "Message".into(), "thread".into(), json!({"chatId": "c1"}).to_string()).await });
    bound(gate.entered()).await;
    let seen = rig.land_unsettled("Message", json!({"id": "m404", "deleted": true})).await;
    let ReadValue::Event { before, after, id, .. } = seen else { unreachable!() };
    assert_eq!((id.as_str(), before.is_none(), after.is_none()), ("m404", true, true), "the event carries no before and no after");
    gate.release();
    bound(load).await.unwrap().unwrap();
    rig.settle().await;
    assert!(row_of(&rows, "m404").is_none(), "the tombstone keeps the late page from bringing the row back");
    rig.database.close_now();
}

/// P-FEED-34: an event on a row with an open operation keeps the field of the operation local and takes the other fields from the event; a row hidden by a pending `destroy` does not come back.
#[tokio::test(flavor = "multi_thread")]
async fn feed_event_keeps_pending_fields() {
    let (rig, rows) = thread_rig().await;
    let epoch = rig.database.session_epoch();
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "base", "at": 1})).await;
    rig.land("Message", json!({"id": "m2", "chatId": "c1", "text": "base", "at": 2})).await;
    let edit = rig.world.hold_next("MessageEdit");
    let delete = rig.world.hold_next("MessageDelete");
    bound(rig.database.act(epoch, "Message".into(), "m1".into(), "update".into(), json!({"text": "local"}).to_string(), json!({"text": "local"}).to_string())).await.unwrap();
    bound(rig.database.destroy(epoch, "Message".into(), "m2".into())).await.unwrap();
    bound(edit.entered()).await;
    rig.land_unsettled("Message", json!({"id": "m2", "chatId": "c1", "text": "server", "at": 8})).await;
    rig.land_unsettled("Message", json!({"id": "m1", "chatId": "c1", "text": "server", "at": 7})).await;
    settled(&rig, "the event reaches the row stream", || row_of(&rows, "m1").is_some_and(|row| cell(&rig, "Message", &row, "at") == int(7))).await;
    let kept = row_of(&rows, "m1").unwrap();
    assert_eq!((cell(&rig, "Message", &kept, "text"), cell(&rig, "Message", &kept, "at")), (text("local"), int(7)), "the field of the open operation stays local and the other field comes from the event");
    assert!(row_of(&rows, "m2").is_none(), "the row hidden by the pending destroy does not come back");
    edit.release();
    delete.release();
    rig.database.close_now();
}
