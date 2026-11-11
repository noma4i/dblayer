use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::add_moment;
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::ReadValue;
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
