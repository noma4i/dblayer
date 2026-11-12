use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::crash_image_support::{copy_tree, files_of, over_images, reopen, saved_pair, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::Stand;

fn copy_image(from: &std::path::Path, to: &std::path::Path) {
    for suffix in ["", "-wal"] {
        let (source, target) = (format!("{}{suffix}", from.display()), format!("{}{suffix}", to.display()));
        if std::path::Path::new(&source).exists() { std::fs::copy(source, target).unwrap(); }
    }
    copy_tree(&files_of(from), &files_of(to));
}

/// A process killed while an interactive call is in flight leaves a file that resends the same pair and hands the answer over once, and a process killed at any commit of that resend leaves a file that resends the pair when no answer was saved, sends nothing when it was, and hands the answer over once.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_during_an_interactive_call_hands_its_answer_over_once() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let pending = directory.path().join("pending.sqlite");
    let secret = Arc::new(Mutex::new(None));
    let taken = Arc::new(AtomicBool::new(false));
    {
        let (live, pending, secret, taken, credentials) = (live.clone(), pending.clone(), secret.clone(), taken.clone(), stand.rig.credentials.clone());
        stand.rig.server.on_request(move || {
            if taken.swap(true, Ordering::SeqCst) { return }
            copy_image(&live, &pending);
            *secret.lock().unwrap() = credentials.inner.value();
        });
    }
    let database = stand.open(Some(&live), &OPERATIONS).await;
    let secret = secret.lock().unwrap().clone().expect("the call went out");
    let answer = serde_json::json!({"nextStep": "authenticated"});
    let pair = saved_pair(&pending).expect("the file taken while the call was in flight holds its pair");

    rusqlite::Connection::open(&pending).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&pending);
    let (resumed_stand, resumed) = reopen(&pending, &secret, &OPERATIONS).await;
    bound(crate::ffi::settled(&resumed)).await.unwrap();
    assert_eq!(resumed_stand.rig.server.seen().iter().filter(|request| request["requestId"] == pair.as_str()).count(), 1, "the resumed process resends the pair");
    let (capture, files) = base.end(&pending);

    let answered = over_images(&capture, &files, |index, image| {
        let (secret, pair, answer) = (secret.clone(), pair.clone(), answer.clone());
        async move {
            let saved = saved_pair(&image);
            let (fresh, reopened) = reopen(&image, &secret, &OPERATIONS).await;
            bound(crate::ffi::settled(&reopened)).await.unwrap();
            let sent: Vec<_> = fresh.rig.server.seen().into_iter().filter(|request| request["requestId"] == pair.as_str()).collect();
            assert_eq!(sent.len(), usize::from(saved.is_some()), "cut {index}: the pair goes out again only when no answer was saved");
            let first = bound(reopened.interrupted_session_answer()).await.unwrap().map(|body| serde_json::from_str::<serde_json::Value>(&body).unwrap());
            assert_eq!(first, Some(answer), "cut {index}: the answer of the interrupted call");
            assert_eq!(bound(reopened.interrupted_session_answer()).await.unwrap(), None, "cut {index}: the answer is handed over once");
            bound(reopened.close_database()).await.unwrap();
            saved.is_none()
        }
    }).await;
    assert!(!answered[0] && *answered.last().unwrap(), "the first cut has the call in flight and the last has its answer: {answered:?}");
    resumed.close_now();
    database.close_now();
}
