use std::sync::Arc;

use serde_json::{json, Value};

use super::rig_tests::{bound, Fault, Rig};
use super::flow_fixtures::{one_revision, add_chat, add_moment, add_profile, cell, int, land_delete, land_event};
use super::upload_fixtures::add_moment_file;
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::upload_wire::WireUploadRequest;
use crate::owners::write::upload::UploadFile;
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::WireOperationOutcome;
use crate::test_wait::yield_until;

const FEED: &str = r#"{"model":"Moment","relation":"feed","params":{}}"#;
const BY_USER: &str = r#"{"model":"Moment","where":{"type":"equal","field":"userId","value":"u1"}}"#;
const COMPASS: &str = r#"{"model":"Moment","order":[{"field":"unreadSimilarMomentsCount","direction":"desc"}]}"#;

async fn rig() -> Rig {
    Rig::encrypted_edited(false, false, false, |models| { add_chat(models); add_profile(models); add_moment(models); add_moment_file(models); }).await
}

async fn watch(rig: &Rig, query: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(query, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    rows
}

fn field(rig: &Rig, rows: &Rows, id: &str, name: &str) -> Option<Cell> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).map(|row| cell(rig, "Moment", row, name))
}

fn ids(rows: &Rows) -> Vec<String> { rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect() }

fn first_seen_at(rig: &Rig, rows: &Rows, id: &str, name: &str, value: Cell) -> i64 {
    rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == id && cell(rig, "Moment", row, name) == value)).map(|seen| seen.revision).unwrap_or(-1)
}

fn moment(id: &str, user: &str, visits: i64, unread: i64) -> Value { json!({"id": id, "userId": user, "text": id, "reacted": false, "visitsCount": visits, "unreadSimilarMomentsCount": unread}) }

async fn same_revision(rig: &Rig, places: &[&Arc<Rows>], id: &str, name: &str, value: Cell, floor: i64) {
    let revisions: Vec<_> = places.iter().map(|rows| first_seen_at(rig, rows, id, name, value.clone())).collect();
    assert!(revisions.iter().all(|revision| *revision >= floor), "a list showed the change before its commit {revisions:?}, floor {floor}\n{}", places.iter().map(|rows| rows.trace()).collect::<Vec<_>>().join("\n"));
    let rows: Vec<&Rows> = places.iter().map(|rows| &***rows).collect();
    one_revision(&rows).await;
}

/// P-APP-22: a reaction changes the counter in every list of moments at one revision on the local write, the answer and an event.
#[tokio::test(flavor = "multi_thread")]
async fn flow_moment_reaction() {
    let rig = rig().await;
    rig.login_ios("A").await;
    land_event(&rig, "Moment", moment("m1", "u1", 2, 0));
    let (feed, by_user, compass) = (watch(&rig, FEED).await, watch(&rig, BY_USER).await, watch(&rig, COMPASS).await);
    let places = [&feed, &by_user, &compass];
    yield_until("every list shows m1", || places.iter().all(|rows| rows.len() == 1)).await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_then("MomentReact", Fault::Answer(json!({"data":{"momentReact":{"__typename":"Moment","id":"m1","reacted":true,"visitsCount":3}}})));

    let receipt = bound(rig.database.act(epoch, "Moment".into(), "m1".into(), "react".into(), json!({"reacted": true, "visitsCount": 3}).to_string(), "{}".into())).await.unwrap();
    hold.entered_for("the answer").await;
    yield_until("every list shows 3", || places.iter().all(|rows| field(&rig, rows, "m1", "visitsCount") == Some(int(3)))).await;
    same_revision(&rig, &places, "m1", "visitsCount", int(3), receipt.revision).await;
    let floor = receipt.revision;
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    rig.settle().await;
    assert!(places.iter().all(|rows| field(&rig, rows, "m1", "visitsCount") == Some(int(3))));

    land_event(&rig, "Moment", moment("m1", "u1", 4, 0));
    yield_until("an event sets 4 in every list", || places.iter().all(|rows| field(&rig, rows, "m1", "visitsCount") == Some(int(4)))).await;
    same_revision(&rig, &places, "m1", "visitsCount", int(4), floor).await;
    rig.database.close_now();
}

/// P-APP-23: reading the compass zeroes the unread counter of a moment at once, the row drops down the compass order at the same revision, and the badge of the account falls after the refetch of the profile.
#[tokio::test(flavor = "multi_thread")]
async fn flow_compass_unread() {
    let rig = rig().await;
    rig.world.set_profile(json!({"unreadCompassCount": 4}));
    rig.login_ios("A").await;
    land_event(&rig, "Moment", moment("m1", "u1", 0, 3));
    land_event(&rig, "Moment", moment("m2", "u2", 0, 1));
    let compass = watch(&rig, COMPASS).await;
    yield_until("the compass leads with m1", || ids(&compass) == ["m1", "m2"]).await;
    let profile = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"User","relation":"profile","params":{}}"#, profile.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    let badge = |rows: &Rows| rows.0.lock().unwrap().iter().find(|row| row.id == "user").map(|row| cell(&rig, "User", row, "unreadCompassCount"));
    yield_until("the badge shows 4", || badge(&profile) == Some(int(4))).await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_then("MomentReadCompass", Fault::Answer(json!({"data":{"momentReadCompass":{"__typename":"Moment","id":"m1","unreadSimilarMomentsCount":0}}})));

    let receipt = bound(rig.database.act(epoch, "Moment".into(), "m1".into(), "read_compass".into(), json!({"unreadSimilarMomentsCount": 0}).to_string(), "{}".into())).await.unwrap();
    hold.entered_for("the answer").await;
    yield_until("m1 sank below m2", || ids(&compass) == ["m2", "m1"]).await;
    same_revision(&rig, &[&compass], "m1", "unreadSimilarMomentsCount", int(0), receipt.revision).await;
    rig.world.set_profile(json!({"unreadCompassCount": 1}));
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the badge falls", || badge(&profile) == Some(int(1))).await;
    assert_eq!(ids(&compass), ["m2", "m1"]);
    rig.database.close_now();
}

/// P-APP-24: a published moment stands in the lists at the revision of the receipt and is replaced by the answer without a second card, a destroy and a delete event take it out of every list.
#[tokio::test(flavor = "multi_thread")]
async fn flow_moment_publish_delete() {
    let rig = rig().await;
    rig.login_ios("A").await;
    let (feed, by_user) = (watch(&rig, FEED).await, watch(&rig, BY_USER).await);
    let places = [&feed, &by_user];
    let epoch = rig.database.session_epoch();
    rig.world.answer_with("MomentCreateFile", json!({"data":{"momentCreateFile":{"__typename":"Moment","id":"m9","userId":"u1","text":"new"}}}));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("moment.jpg");
    std::fs::write(&path, b"jpeg").unwrap();
    let values = json!({"userId": "u1", "text": "new"});
    let mut file = UploadFile::new(path.to_str().unwrap());
    file.variables = values.as_object().unwrap().clone();
    let publish = CommandRequest { epoch, model: "Moment".into(), values_json: values.to_string(), args_json: "{}".into(), upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest::of(&file) }) };
    let hold = rig.world.hold_next("PUT");

    let receipt = bound(rig.database.send(publish)).await.unwrap();
    hold.entered_for("the file in the network").await;
    yield_until("the new moment stands in every list", || places.iter().all(|rows| rows.len() == 1)).await;
    let temporary = receipt.optimistic_value.clone().unwrap().id;
    assert!(places.iter().all(|rows| ids(rows) == [temporary.clone()]));
    assert!(places.iter().all(|rows| rows.0.lock().unwrap().iter().all(|row| row.uploads.iter().any(|entry| entry.action == "create_file"))), "the card shows its upload");
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the answer replaced the card", || places.iter().all(|rows| ids(rows) == ["m9"])).await;
    assert!(places.iter().all(|rows| rows.1.lock().unwrap().iter().all(|seen| seen.size <= 1)), "a list showed the card twice");

    rig.world.answer_with("MomentDelete", json!({"data":{"momentDelete":{"id":"m9"}}}));
    let destroy = bound(rig.database.destroy(epoch, "Moment".into(), "m9".into())).await.unwrap();
    yield_until("the destroy empties every list", || places.iter().all(|rows| rows.emptied())).await;
    assert!(matches!(bound(rig.database.outcome(destroy)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));

    land_event(&rig, "Moment", moment("m5", "u1", 0, 0));
    yield_until("an event card stands", || places.iter().all(|rows| rows.len() == 1)).await;
    land_delete(&rig, "Moment", "m5");
    yield_until("the delete event empties every list", || places.iter().all(|rows| rows.emptied())).await;
    rig.database.close_now();
}
