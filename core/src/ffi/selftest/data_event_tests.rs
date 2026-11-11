use std::sync::Arc;

use serde_json::{json, Value};

use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::ReadValue;

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
