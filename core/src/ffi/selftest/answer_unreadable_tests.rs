use super::crash_image_support::{drained, queue_now};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::{plain, Stand, CREATE_MOMENT};
use crate::ffi::DbError;

const CREATED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","text":"first","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const UPDATE_MOMENT: (&str, &str) = ("UpdateMoment", "mutation UpdateMoment { updateMoment { id uuid text likedByMe media createdAt } }");
const UPDATED_UNREADABLE: &str = r#"{"data":{"updateMoment":{"__typename":"Moment","id":"m77","uuid":null,"text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const CREATED_UNREADABLE: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":null,"text":"second","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// R14 (D-470): the server accepted an `update` and the row of its answer has a value of the wrong type. Observable: the outcome of the receipt, the queue and the requests the server saw. Assert: the receipt is confirmed, the queue is empty, the update is sent once. Catches: a local decoding error that cancels the server outcome as a refusal (rollback or Failed).
#[tokio::test(flavor = "multi_thread")]
async fn an_update_whose_answer_row_does_not_decode_is_confirmed() {
    let operations = [OPERATIONS[0], OPERATIONS[1], OPERATIONS[2], CREATE_MOMENT, UPDATE_MOMENT];
    let stand = Stand::new();
    let database = stand.open(None, &operations).await;
    let epoch = database.session_epoch();
    stand.script.data("CreateMoment", CREATED);
    let created = bound(database.send(plain("Moment", r#"{"text":"first"}"#, epoch))).await.unwrap();
    bound(database.confirmed(created, false)).await.unwrap();
    drained(&database).await;
    stand.script.data("UpdateMoment", UPDATED_UNREADABLE);
    let receipt = bound(database.update(epoch, "Moment".into(), "m77".into(), r#"{"text":"edited"}"#.into(), "{}".into())).await.unwrap();
    let outcome = bound(database.confirmed(receipt, false)).await;
    assert!(outcome.is_ok(), "the accepted update is confirmed: {outcome:?}");
    drained(&database).await;
    assert_eq!(queue_now(&database).await, 0, "the intent is removed");
    assert_eq!(stand.script.count("UpdateMoment"), 1, "the update is not sent again");
    database.close_now();
}

/// R14 (D-470), `outbox.md` Sending x Response: the server accepted a `create` and the row of its answer does not decode. Assert: the receipt is confirmed with no row, a queued update on the temporary id ends refused without a request (P-OUT-57), the optimistic row with the temporary id is gone, no row with the real id is written (the reread brings it). Catches: a refusal path that rolls the create back or keeps it as Failed.
#[tokio::test(flavor = "multi_thread")]
async fn a_create_whose_answer_row_does_not_decode_is_confirmed_without_a_row() {
    let operations = [OPERATIONS[0], OPERATIONS[1], OPERATIONS[2], CREATE_MOMENT, UPDATE_MOMENT];
    let stand = Stand::new();
    let database = stand.open(None, &operations).await;
    let epoch = database.session_epoch();
    let held = stand.script.hold("CreateMoment", CREATED_UNREADABLE);
    let sent = bound(database.send(plain("Moment", r#"{"text":"second"}"#, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    held.entered().await;
    let dependent = bound(database.update(epoch, "Moment".into(), temporary.clone(), r#"{"text":"third"}"#.into(), "{}".into())).await.unwrap();
    held.release();
    let outcome = bound(database.confirmed(sent, false)).await;
    assert!(matches!(&outcome, Ok(confirmed) if confirmed.value.is_none()), "the accepted create is confirmed with no row: {outcome:?}");
    let ended = bound(database.confirmed(dependent, false)).await;
    assert!(matches!(ended, Err(DbError::Rejected { kept: false, .. })), "the update queued on the removed temporary row ends refused: {ended:?}");
    assert_eq!(stand.script.count("UpdateMoment"), 0, "the update is never sent");
    drained(&database).await;
    assert_eq!(queue_now(&database).await, 0, "the intent is removed");
    assert!(database.peek_row(epoch, "Moment".into(), temporary).await.unwrap().is_none(), "the optimistic row is removed");
    assert!(database.peek_row(epoch, "Moment".into(), "m78".into()).await.unwrap().is_none(), "no row of the answer is written");
    assert_eq!(stand.script.count("CreateMoment"), 1, "the create is not sent again");
    database.close_now();
}
