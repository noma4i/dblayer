//! The frames of a row whose upload mutation was refused and is retried: once the retry shows a non-failed state, no later frame shows the old failure again (report of the Android app on 6.47.0, P-XFER-14, P-OUT-62).

use std::sync::{Arc, Mutex};

use super::rig_tests::bound;
use super::upload_stand::{clip, Answer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::tape::{Reader, Rows, StreamDelivery};
use crate::ffi::{DbError, ReadFetch, ReadRequest};

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

#[derive(Default)]
struct Frames {
    rows: Rows,
    seen: Mutex<Vec<(Option<String>, String)>>,
}

impl Reader for Frames {
    fn delivered(&self, delivery: StreamDelivery) {
        self.rows.delivered(delivery);
        let shown = self.rows.0.lock().unwrap();
        if let Some(row) = shown.first() {
            let upload = row.uploads.iter().find(|entry| entry.action == "upload_clip").map(|entry| format!("{:?}", entry.upload.status));
            self.seen.lock().unwrap().push((upload, format!("{:?}", row.sync.state)));
        }
    }
}

fn failed((upload, sync): &(Option<String>, String)) -> bool { upload.as_deref() == Some("Failed") || sync == "Failed" }

/// After `retry` of an upload whose final mutation was refused, the frames of the row never go back to `Failed` once a frame of the retry left it, and the retry lands.
#[tokio::test(flavor = "multi_thread")]
async fn a_retried_upload_never_shows_the_old_failure_again() {
    for refusal in [Answer::Status(422), Answer::Data(r#"{"errors":[{"message":"Publication rejected"}]}"#.into())] {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("photo.jpg");
    std::fs::write(&file, b"jpeg").unwrap();
    stand.script.data("CreateBlob", BLOB);
    stand.script.enqueue("CreateMoment", refusal);
    let sent = bound(database.send(clip("Moment", r#"{"text":"t"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let id = sent.optimistic_value.clone().unwrap().id;
    let frames = Arc::new(Frames::default());
    drop(database.watch(ReadRequest::Entity { model: "Moment".into(), id: id.clone(), fetch: ReadFetch::Never }, frames.clone()));
    assert!(matches!(bound(database.confirmed(sent, false)).await, Err(DbError::Rejected { kept: true, .. })));
    crate::test_wait::settled(&database, "the row shows the failure", || frames.seen.lock().unwrap().last().is_some_and(failed)).await;
    let before = frames.seen.lock().unwrap().len();
    stand.script.data("CreateMoment", LANDED);
    let retried = bound(database.retry(epoch, "Moment".into(), id, Some("upload_clip".into()), None)).await.unwrap();
    bound(database.confirmed(retried, false)).await.unwrap();
    let life = database.life_handle();
    bound(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
    let after: Vec<_> = frames.seen.lock().unwrap()[before..].to_vec();
    let left = after.iter().position(|frame| !failed(frame));
    assert!(left.is_some(), "the retry leaves the failure: {after:?}");
    assert!(!after[left.unwrap()..].iter().any(failed), "a frame after the retry left the failure shows it again: {after:?}");
    database.close_now();
    }
}
