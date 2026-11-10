use serde_json::{json, Value};
use super::stand::{message, moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::{ReadFetch, ReadValue};
use crate::test_wait::settled;

fn media_row(mut row: Value, status: &str, progress: i64) -> Value {
    row["media"] = json!({"transcodeStatus": status, "transcodeProgress": progress});
    row
}

fn media_status(stand: &Stand, rows: &Rows, model: &str, id: &str) -> Option<String> {
    let rows = rows.0.lock().unwrap();
    let row = rows.iter().find(|row| row.id == id)?;
    let spec = stand.database.db.spec(model).unwrap();
    let members = &spec.field("media").unwrap().fields;
    let index = members.iter().position(|member| member.name == "transcodeStatus").unwrap();
    let Cell::Seq { items } = stand.cell(model, row, "media") else { return None };
    match &items[index] {
        Cell::Text { value } => Some(value.clone()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_message_transcode_event_updates_reader() {
    let stand = Stand::open().await;
    let (events, token) = stand.events("Message").await;
    let rows = stand.entity("Message", "m1", ReadFetch::Never);
    let asked = stand.requests("MessageNode");
    let before = media_row(message("m1", "c1", "clip"), "PROGRESSING", 40);
    stand.push(&token, json!({"area":"MESSAGE", "kind":"UPDATED", "entityId":"m1", "message":before}));
    settled(&stand, "message begins transcoding", || media_status(&stand, &rows, "Message", "m1").as_deref() == Some("PROGRESSING")).await;
    let after = media_row(message("m1", "c1", "clip"), "COMPLETED", 100);
    stand.push(&token, json!({"area":"MESSAGE", "kind":"UPDATED", "entityId":"m1", "message":after}));
    settled(&stand, "message is transcoded", || media_status(&stand, &rows, "Message", "m1").as_deref() == Some("COMPLETED")).await;
    assert_eq!(stand.requests("MessageNode"), asked, "payload landing needs no node query");
    drop(events);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_moment_transcode_event_rereads_node() {
    let stand = Stand::open().await;
    stand.answer("MomentNode", json!({"node":media_row(moment("m1", 0), "PROGRESSING", 40)}));
    let (events, token) = stand.events("Moment").await;
    let rows = stand.entity("Moment", "m1", ReadFetch::Stale);
    settled(&stand, "moment begins transcoding", || media_status(&stand, &rows, "Moment", "m1").as_deref() == Some("PROGRESSING")).await;
    let asked = stand.requests("MomentNode");
    stand.answer("MomentNode", json!({"node":media_row(moment("m1", 0), "COMPLETED", 100)}));
    stand.push(&token, json!({"area":"MOMENT", "kind":"UPDATED", "entityId":"m1"}));
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Event { .. })));
    settled(&stand, "moment is transcoded", || media_status(&stand, &rows, "Moment", "m1").as_deref() == Some("COMPLETED")).await;
    assert_eq!(stand.requests("MomentNode"), asked + 1);
}
