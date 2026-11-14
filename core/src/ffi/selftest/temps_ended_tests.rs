use super::crash_image_support::{queue_now, reopen};
use super::rig_tests::bound;
use super::upload_stand::{clip, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::DbError;

const UPDATE_MOMENT: (&str, &str) = ("UpdateMoment", "mutation UpdateMoment { updateMoment { id uuid likedByMe media createdAt } }");
const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const UPDATED_LANDED: &str = r#"{"data":{"updateMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const UPDATED: &str = r#"{"data":{"updateMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// The requests the server saw that carry the temporary id in their variables.
fn carrying(stand: &Stand, temporary: &str) -> Vec<String> {
    stand.script.names().into_iter().collect::<std::collections::BTreeSet<_>>().into_iter().filter(|name| stand.script.variables(name).iter().any(|variables| variables.to_string().contains(temporary))).collect()
}

/// P-OUT-57 (D-449): the temporary row of an upload ends by `cancel_upload`, a batch of the Transfer, not of the Outbox. Observable: the outcome of an `update` queued on that row and the requests the server saw. Catches: an end of a temporary id that the Outbox does not learn, so the queued `update` goes out with the temporary id.
#[tokio::test(flavor = "multi_thread")]
async fn temps_ended_operation_on_a_row_ended_by_a_cancelled_upload_is_refused() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let operations = [CREATE_MOMENT, UPDATE_MOMENT];
    let database = stand.open(None, &operations).await;
    let epoch = database.session_epoch();
    stand.script.data("UpdateMoment", UPDATED);
    let held = stand.script.hold("CreateBlob", BLOB);
    let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    held.entered().await;
    let update = bound(database.update(epoch, "Moment".into(), temporary.clone(), r#"{"text":"B"}"#.into(), "{}".into())).await.unwrap();
    assert!(bound(database.cancel_upload(epoch, "Moment".into(), temporary.clone(), "upload_clip".into())).await.unwrap());
    held.release();
    let ended = bound(database.confirmed(update, false)).await;
    assert!(matches!(ended, Err(DbError::Rejected { kept: false, .. })), "the update on the ended row ends refused: {ended:?}");
    assert_eq!(carrying(&stand, &temporary), Vec::<String>::new(), "no request carries the temporary id");
    database.close_now();
}

/// P-OUT-57, D-439 (D-449): the end of the temporary id survives `close` and `open`: the `update` that waited on the row is restored and ends without a request. Observable: the queue and the requests of the fresh process. Catches: a fate of the temporary id that is never written.
#[tokio::test(flavor = "multi_thread")]
async fn temps_ended_restored_operation_on_an_ended_row_sends_nothing() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let operations = [CREATE_MOMENT, UPDATE_MOMENT];
    let database = stand.open(Some(&live), &operations).await;
    let epoch = database.session_epoch();
    database.set_online(false).await.unwrap();
    let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    bound(database.update(epoch, "Moment".into(), temporary.clone(), r#"{"text":"B"}"#.into(), "{}".into())).await.unwrap();
    assert!(bound(database.cancel_upload(epoch, "Moment".into(), temporary.clone(), "upload_clip".into())).await.unwrap());
    bound(database.close_database()).await.unwrap();
    let secret = stand.rig.credentials.inner.value().unwrap();

    let (fresh, reopened) = reopen(&live, &secret, &operations, |fresh| fresh.script.data("UpdateMoment", UPDATED)).await;
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(queue_now(&reopened).await, 0, "the restored update ended");
    assert_eq!(carrying(&fresh, &temporary), Vec::<String>::new(), "no request carries the temporary id");
    assert_eq!(fresh.script.count("UpdateMoment"), 0, "the update is not sent");
    bound(reopened.close_database()).await.unwrap();
}

/// P-OUT-56 (D-449): an `update` of a temporary row held by an upload waits for the fate of the row and goes out with the server id the final mutation of the upload brings. Observable: the requests the server saw and the outcome of the `update`. Catches: an operation that goes out with the temporary id before its row exists on the server, and a wait that never ends after the row landed.
#[tokio::test(flavor = "multi_thread")]
async fn temps_ended_update_of_a_row_an_upload_holds_goes_out_with_the_server_id() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let operations = [CREATE_MOMENT, UPDATE_MOMENT];
    let database = stand.open(None, &operations).await;
    let epoch = database.session_epoch();
    stand.script.data("CreateMoment", LANDED);
    stand.script.data("UpdateMoment", UPDATED_LANDED);
    let held = stand.script.hold("CreateBlob", BLOB);
    let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    held.entered().await;
    let update = bound(database.update(epoch, "Moment".into(), temporary.clone(), r#"{"text":"B"}"#.into(), "{}".into())).await.unwrap();
    held.release();
    assert!(bound(database.confirmed(update, false)).await.is_ok(), "the update lands after the upload");
    assert_eq!(carrying(&stand, &temporary), Vec::<String>::new(), "no request carries the temporary id");
    let names = stand.script.names();
    let at = |operation: &str| names.iter().position(|name| name == operation);
    assert_eq!((stand.script.count("UpdateMoment"), at("CreateMoment").zip(at("UpdateMoment")).map(|(create, update)| create < update)), (1, Some(true)), "the update goes out once, after the final mutation of the upload landed the row: {names:?}");
    database.close_now();
}
