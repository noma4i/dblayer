use std::sync::Arc;

use super::crash_image_call_tests::copy_image;
use super::crash_image_support::{advancing, drained, reopen_timed, saved_pair, timed_stand};
use super::rig_tests::bound;
use super::upload_stand::{clip, plain, Answer, Hold, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::{Database, TestClock};
use crate::owners::write::upload::MEBIBYTE;
use std::path::Path;

const LANDED_B: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":"v","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const LANDED_CLIP: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m79","uuid":"w","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const REFUSED: &str = r#"{"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}"#;
const ABORTED: &str = r#"{"data":{"abortMultipart":{"ok":true}}}"#;

#[derive(Clone, Copy, Debug)]
enum Holder {
    /// A step of an upload (`CreateBlob`) of a transfer.
    TransferStep,
    /// The abort of the multipart upload of a discarded transfer.
    AbortDebt,
    /// The pair released by an operation that ended `Failed`.
    ReleasedPair,
}

/// The pair the image holds: the request id and the sequence.
fn saved(image: &Path) -> (String, i64) {
    let request_id = saved_pair(image).expect("the image holds a pair");
    let sequence = rusqlite::Connection::open(image).unwrap().query_row("SELECT sequence FROM encrypted_pending", [], |row| row.get::<_, i64>(0)).unwrap();
    (request_id, sequence)
}

fn pending_rows(image: &Path) -> i64 {
    rusqlite::Connection::open(image).unwrap().query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get(0)).unwrap()
}

/// The image of a process killed while the pair of `origin` was in flight, with another write saved behind it (the released pair: the write comes after the open).
async fn image_of(origin: Holder, directory: &Path, stand: &Stand, clock: &Arc<TestClock>) -> (std::path::PathBuf, String) {
    let live = directory.join("live.sqlite");
    let source = directory.join("source.bin");
    std::fs::File::create(&source).unwrap().set_len(match origin { Holder::AbortDebt => 11 * MEBIBYTE, _ => 4 }).unwrap();
    let database = stand.open_with(Some(&live), &[CREATE_MOMENT], Some(clock.clone())).await;
    let secret = stand.rig.credentials.inner.value().unwrap();
    let epoch = database.session_epoch();
    match origin {
        Holder::TransferStep => {
            let hold = Arc::new(Hold::default());
            stand.script.enqueue("CreateBlob", Answer::Held(hold.clone(), Box::new(Answer::Lost)));
            bound(database.send(clip("Moment", r#"{"text":"clip"}"#, "upload_clip", &source, epoch))).await.unwrap();
            hold.entered().await;
            bound(database.send(plain("Moment", r#"{"text":"B"}"#, epoch))).await.unwrap();
        }
        Holder::AbortDebt => {
            stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"multipartUpload":{"key":"key","uploadId":"upload"}}}}"#);
            let presign = stand.script.hold("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#);
            let sent = bound(database.send(clip("Moment", r#"{"text":"clip"}"#, "upload_clip", &source, epoch))).await.unwrap();
            let id = sent.optimistic_value.unwrap().id;
            presign.entered().await;
            database.set_online(false).await.unwrap();
            presign.dropped().await;
            bound(database.discard(epoch, "Moment".into(), id, Some("upload_clip".into()))).await.unwrap();
            presign.release();
            super::crash_image_support::checkpoint_live(&database, &live).await;
            let owed = directory.join("owed.sqlite");
            copy_image(&live, &owed);
            database.close_now();
            let abort = Arc::new(Hold::default());
            let held = abort.clone();
            let (_second, reopened, _clock) = reopen_timed(&owed, &secret, &[CREATE_MOMENT], move |second| { second.script.data("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#); second.script.enqueue("AbortMultipart", Answer::Held(held, Box::new(Answer::Lost))); }, false).await;
            abort.entered().await;
            let epoch = reopened.session_epoch();
            bound(reopened.send(plain("Moment", r#"{"text":"B"}"#, epoch))).await.unwrap();
            super::crash_image_support::checkpoint_live(&reopened, &owed).await;
            let image = directory.join("image.sqlite");
            copy_image(&owed, &image);
            reopened.close_now();
            return (image, secret);
        }
        Holder::ReleasedPair => {
            for _ in 0..40 { stand.script.enqueue("CreateMoment", Answer::Lost); }
            let released = stand.script.hold("CreateMoment", REFUSED);
            bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
            until_failed(&database, clock).await;
            released.entered().await;
        }
    }
    super::crash_image_support::checkpoint_live(&database, &live).await;
    let image = directory.join("image.sqlite");
    copy_image(&live, &image);
    database.close_now();
    (image, secret)
}

async fn until_failed(database: &Database, clock: &TestClock) {
    let stream = database.subscribe_values(database.session_epoch(), crate::ffi::ReadRequest::Operations);
    let wait = async {
        loop {
            match bound(stream.pull_next()).await {
                Some(crate::ffi::ReadValue::Operations { items, .. }) if items.iter().any(|item| item.status == crate::ffi::OperationQueueStatus::Failed) => return,
                Some(crate::ffi::ReadValue::Operations { .. }) => {}
                other => panic!("the operation queue ended: {other:?}"),
            }
        }
    };
    advancing(clock, wait).await;
}

/// sess_held_pair_of_every_origin_goes_first_after_restart: a process killed with the pair of a transfer step, of an abort debt or of a released operation in flight, and writes behind it, opens: the pair goes first under the same request id and sequence, the writes go after its answer, every request reaches the server once and no pair is left.
#[tokio::test(flavor = "multi_thread")]
async fn sess_held_pair_of_every_origin_goes_first_after_restart() {
    for origin in [Holder::TransferStep, Holder::AbortDebt, Holder::ReleasedPair] {
        let (stand, clock) = timed_stand();
        let stand = if matches!(origin, Holder::ReleasedPair) { stand.keep_moment() } else { stand };
        let directory = tempfile::tempdir().unwrap();
        let (image, secret) = image_of(origin, directory.path(), &stand, &clock).await;
        let pair = saved(&image);
        let (fresh, reopened, clock) = reopen_timed(&image, &secret, &[CREATE_MOMENT], |fresh| {
            match origin {
                Holder::TransferStep => { fresh.script.data("CreateBlob", BLOB); fresh.script.data("CreateMoment", LANDED_B); fresh.script.data("CreateMoment", LANDED_CLIP); }
                Holder::AbortDebt => { fresh.script.data("AbortMultipart", ABORTED); fresh.script.data("CreateMoment", LANDED_B); }
                Holder::ReleasedPair => { fresh.script.data("CreateMoment", REFUSED); fresh.script.data("CreateMoment", LANDED_B); }
            }
        }, matches!(origin, Holder::ReleasedPair)).await;
        if matches!(origin, Holder::ReleasedPair) {
            let epoch = reopened.session_epoch();
            bound(reopened.send(plain("Moment", r#"{"text":"B"}"#, epoch))).await.unwrap();
        }
        match origin {
            Holder::ReleasedPair => advancing(&clock, crate::test_wait::settled(&reopened, "the write behind the pair landed", || reopened.peek_row_now(reopened.session_epoch(), "Moment".into(), "m78".into()).unwrap().is_some())).await,
            _ => advancing(&clock, drained(&reopened)).await,
        }
        let names = fresh.script.names();
        let first = match origin { Holder::TransferStep => "CreateBlob", Holder::AbortDebt => "AbortMultipart", Holder::ReleasedPair => "CreateMoment" };
        assert_eq!(names.first().map(String::as_str), Some(first), "{origin:?}: the pair goes first: {names:?}");
        assert_eq!(fresh.script.operation_pairs(first)[0], (pair.0.clone(), Some(pair.1)), "{origin:?}: the pair keeps its request id and sequence");
        let once: Vec<_> = names.iter().filter(|name| name.as_str() == first).collect();
        assert_eq!(once.len(), if matches!(origin, Holder::ReleasedPair) { 2 } else { 1 }, "{origin:?}: the pair is sent once: {names:?}");
        let writes = fresh.script.operation_pairs("CreateMoment");
        let distinct: std::collections::BTreeSet<_> = writes.iter().collect();
        assert_eq!(distinct.len(), writes.len(), "{origin:?}: no request is sent twice: {writes:?}");
        bound(reopened.close_database()).await.unwrap();
        assert_eq!(pending_rows(&image), 0, "{origin:?}: no pair is left");
    }
}
