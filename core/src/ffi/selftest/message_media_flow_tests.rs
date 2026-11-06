use std::sync::Arc;

use serde_json::{json, Value};

use super::bridge_tests::message;
use super::rig_tests::{bound, Rig, World};
use super::flow_fixtures::{add_chat, add_media, cell, chat_row, int, land_event, text, texts};
use super::upload_fixtures::add_message_file;
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::wire::WireUploadStatus;
use crate::upload::UploadFile;
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::{ReadFetch, ReadRequest};
use crate::test_wait::yield_until;

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

fn entity(rig: &Rig, model: &str, id: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    drop(rig.database.watch(ReadRequest::Entity { model: model.into(), id: id.into(), fetch: ReadFetch::Never }, rows.clone()));
    rows
}

fn field(rig: &Rig, model: &str, rows: &Rows, id: &str, name: &str) -> Option<Cell> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).map(|row| cell(rig, model, row, name))
}

fn media(url: &str, progress: i64) -> Value { json!({"fileUrl": url, "transcodeProgress": progress}) }

fn event(id: &str, extra: Value) -> Value {
    let mut row = json!({"id": id, "chatId": "c1", "text": "clip", "at": 10});
    row.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    row
}

fn status(rows: &Rows, id: &str) -> Option<WireUploadStatus> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).and_then(|row| row.uploads.iter().find(|entry| entry.action == "send_file").map(|entry| entry.upload.status))
}

fn send_file(epoch: u64, path: &str) -> CommandRequest {
    let values = json!({"chatId": "c1", "text": "clip", "at": 10});
    let mut file = UploadFile::new(path);
    file.variables = values.as_object().unwrap().clone();
    CommandRequest { epoch, model: "Message".into(), values_json: values.to_string(), args_json: "{}".into(), upload: Some(CommandUpload { action: "send_file".into(), request: WireUploadRequest::of(&file) }) }
}

/// P-APP-14: a sent file stands in the thread at once with `uploads["send_file"]`, its local preview survives events of the server, the URL of the server is never emptied and the progress of the transcode never falls, in the thread and in the entity.
#[tokio::test(flavor = "multi_thread")]
async fn flow_message_media_upload() {
    let rig = rig().await;
    rig.world.answer_with("SendMessageFile", json!({"data":{"sendMessageFile":{"__typename":"Message","id":"m1","chatId":"c1","text":"clip","at":10}}}));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("clip.jpg");
    std::fs::write(&path, b"jpeg").unwrap();
    let (rows, last) = (thread(&rig).await, entity(&rig, "Message", "m1"));
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");

    let receipt = bound(rig.database.send(send_file(epoch, path.to_str().unwrap()))).await.unwrap();
    hold.entered_for("the file in the network").await;
    let temporary = receipt.optimistic_value.clone().unwrap().id;
    yield_until("the bubble stands with its upload", || rows.len() == 1 && status(&rows, &temporary).is_some()).await;
    bound(rig.database.set_local(epoch, "Message".into(), temporary, "localPreviewUrl".into(), json!("file:///clip.jpg").to_string())).await.unwrap();
    yield_until("the preview stands at once", || rows.0.lock().unwrap().iter().all(|row| cell(&rig, "Message", row, "localPreviewUrl") == text("file:///clip.jpg"))).await;
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    yield_until("the answer replaced the bubble", || rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>() == ["m1"]).await;
    yield_until("the entity of the answer shows", || last.len() == 1).await;
    yield_until("the preview stands on the row of the server", || field(&rig, "Message", &rows, "m1", "localPreviewUrl") == Some(text("file:///clip.jpg"))).await;
    assert_eq!(status(&rows, "m1"), None, "the upload ended with the answer");

    for (url, progress, shown_url, shown_progress) in [("", 40, "", 40), ("https://cdn/a", 80, "https://cdn/a", 80), ("", 30, "https://cdn/a", 80)] {
        land_event(&rig, "Message", event("m1", json!({"media": media(url, progress)})));
        yield_until("the event reached both places", || [&rows, &last].iter().all(|place| field(&rig, "Message", place, "m1", "media").is_some_and(|media| {
            let words = texts(&media);
            words.contains(&shown_url.to_string()) && words.len() >= usize::from(!shown_url.is_empty())
        }))).await;
        for place in [&rows, &last] {
            let media = field(&rig, "Message", place, "m1", "media").unwrap();
            assert!(format!("{media:?}").contains(&format!("value: {shown_progress}")), "url {url:?} progress {progress}: shows {media:?}");
            assert_eq!(field(&rig, "Message", place, "m1", "localPreviewUrl"), Some(text("file:///clip.jpg")), "an event wiped the local preview");
        }
    }
    rig.database.close_now();
}

/// P-APP-15: opening a gift sets the local flag in the thread and in the entity of the last message at once, and the event of the server that opens it does not clear the flag.
#[tokio::test(flavor = "multi_thread")]
async fn flow_message_gift() {
    let rig = rig().await;
    land_event(&rig, "Message", event("g1", json!({"gift": {"status": "WRAPPED"}})));
    let (rows, last) = (thread(&rig).await, entity(&rig, "Message", "g1"));
    yield_until("both places show the gift", || rows.len() == 1 && last.len() == 1).await;
    let epoch = rig.database.session_epoch();

    bound(rig.database.set_local(epoch, "Message".into(), "g1".into(), "giftOpened".into(), "true".into())).await.unwrap();
    yield_until("both places show the opened flag", || [&rows, &last].iter().all(|place| field(&rig, "Message", place, "g1", "giftOpened") == Some(Cell::Bool { value: true }))).await;

    land_event(&rig, "Message", event("g1", json!({"gift": {"status": "OPENED"}})));
    yield_until("the event opens the gift in both places", || [&rows, &last].iter().all(|place| field(&rig, "Message", place, "g1", "gift").is_some_and(|gift| texts(&gift).contains(&"OPENED".to_string())))).await;
    assert!([&rows, &last].iter().all(|place| field(&rig, "Message", place, "g1", "giftOpened") == Some(Cell::Bool { value: true })), "the event cleared the local flag");
    rig.database.close_now();
}

/// P-APP-17: the local media counters of a chat are written by the app, shown in the chat list, and neither an event nor an answer of the server clears them.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_media_library() {
    let rig = rig().await;
    let chats = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, chats.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    yield_until("the chat shows", || chats.len() == 1).await;
    let epoch = rig.database.session_epoch();

    for (path, value) in [("mediaSequence", 5), ("mediaPhotoCount", 3)] {
        bound(rig.database.set_local(epoch, "Chat".into(), "c1".into(), path.into(), value.to_string())).await.unwrap();
    }
    yield_until("the counters show", || field(&rig, "Chat", &chats, "c1", "mediaSequence") == Some(int(5)) && field(&rig, "Chat", &chats, "c1", "mediaPhotoCount") == Some(int(3))).await;

    land_event(&rig, "Chat", json!({"id": "c1", "name": "renamed", "lastMessageAt": 0, "lastActivityAt": 0, "unreadCount": 1, "pinned": false, "muted": false}));
    yield_until("the event renames the chat", || field(&rig, "Chat", &chats, "c1", "name") == Some(text("renamed"))).await;
    assert_eq!((field(&rig, "Chat", &chats, "c1", "mediaSequence"), field(&rig, "Chat", &chats, "c1", "mediaPhotoCount")), (Some(int(5)), Some(int(3))), "an event cleared the local counters");
    rig.database.close_now();
}

/// P-OUT-22 out_prop_swap_carries_local: the answer that replaces a temporary row keeps the local value the app set on the temporary row, in the thread and in the entity.
#[tokio::test(flavor = "multi_thread")]
async fn out_prop_swap_carries_local() {
    let rig = rig().await;
    let (rows, last) = (thread(&rig).await, entity(&rig, "Message", "m1"));
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("SendMessage");

    let receipt = bound(rig.database.send(message(epoch, "clip", 10))).await.unwrap();
    hold.entered_for("the send").await;
    let temporary = receipt.optimistic_value.clone().unwrap().id;
    bound(rig.database.set_local(epoch, "Message".into(), temporary, "localPreviewUrl".into(), json!("file:///clip.jpg").to_string())).await.unwrap();
    hold.release();
    bound(rig.database.outcome(receipt)).await.unwrap();
    yield_until("the answer replaced the bubble", || rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>() == ["m1"] && last.len() == 1).await;
    yield_until("the preview stands on the row of the answer in both places", || [&rows, &last].iter().all(|place| field(&rig, "Message", place, "m1", "localPreviewUrl") == Some(text("file:///clip.jpg")))).await;
    rig.database.close_now();
}
