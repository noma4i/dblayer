use std::future::Future;

use super::failure_tests::rig;
use super::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};
use crate::test_wait::join_within;

const MARKER_COLUMN: usize = 10_000;

static PARSE_FAILED_ON: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<usize, std::thread::ThreadId>>> = std::sync::LazyLock::new(Default::default);

/// Records the thread that turned a marked parse failure into a `DbError`. The call marks its failure with a column number of at least `MARKER_COLUMN`, so other tests never write here.
pub(in super::super) fn note_parse_failure(error: &serde_json::Error) {
    if error.column() >= MARKER_COLUMN {
        PARSE_FAILED_ON.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(error.column(), std::thread::current().id());
    }
}

type Call = Box<dyn FnOnce(std::sync::Arc<super::Database>, String) -> std::pin::Pin<Box<dyn Future<Output = Result<(), super::DbError>>>> + Send>;

fn request(variables_json: String) -> WireUploadRequest {
    WireUploadRequest { source: crate::ffi::upload_wire::WireSource::File { path: "p".into(), name: None, mime: None }, variables_json }
}

fn json_calls() -> Vec<(&'static str, Call)> {
    let moment = || "Moment".to_string();
    vec![
        ("upload", Box::new(move |db, bad| Box::pin(async move { db.upload(db.session_epoch(), moment(), "r1".into(), "upload".into(), request(bad)).await.map(drop) }))),
        ("upload_create_batch", Box::new(move |db, bad| {
            Box::pin(async move {
                let item = WireUploadItem { values_json: bad, source: crate::ffi::upload_wire::WireSource::File { path: "p".into(), name: None, mime: None }, variables_json: "{}".into() };
                let batch = WireUploadBatch { operation: "o".into(), variables_json: "{}".into() };
                db.upload_create_batch(db.session_epoch(), moment(), vec![item], batch).await.map(drop)
            })
        })),
        ("run", Box::new(move |db, bad| Box::pin(async move { db.run(db.session_epoch(), "Feed".into(), bad).await.map(drop) }))),
        ("load", Box::new(move |db, bad| Box::pin(async move { db.load(db.session_epoch(), moment(), "feed".into(), bad).await }))),
        ("refresh", Box::new(move |db, bad| Box::pin(async move { db.refresh(db.session_epoch(), moment(), Some("feed".into()), bad).await }))),
        ("load_more", Box::new(move |db, bad| Box::pin(async move { db.load_more(db.session_epoch(), moment(), Some("feed".into()), bad).await.map(drop) }))),
        ("load_all", Box::new(move |db, bad| Box::pin(async move { db.load_all(db.session_epoch(), moment(), Some("feed".into()), bad, false).await }))),
        ("edge_values", Box::new(move |db, bad| Box::pin(async move { db.edge_values(db.session_epoch(), moment(), Some("feed".into()), bad, vec!["r1".into()]).await.map(drop) }))),
    ]
}

#[tokio::test]
async fn calls_with_json_arguments_parse_them_off_the_caller_thread() {
    let (db, _) = rig(Ok("{}")).await;
    let mut on_caller = Vec::new();
    let mut unobserved = Vec::new();
    for (index, (name, call)) in json_calls().into_iter().enumerate() {
        let column = MARKER_COLUMN + index;
        let bad = format!("{{{}", " ".repeat(column - 1));
        let db = db.clone();
        let (caller, outcome) = join_within(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let outcome = runtime.block_on(call(db, bad));
            (std::thread::current().id(), outcome)
        }));
        assert!(matches!(outcome, Err(super::DbError::Invalid { .. })), "{name} did not fail on the invalid JSON: {outcome:?}");
        match PARSE_FAILED_ON.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&column) {
            None => unobserved.push(name),
            Some(parsed_on) if parsed_on == caller => on_caller.push(name),
            Some(_) => {}
        }
    }
    assert!(unobserved.is_empty(), "no parse failure was observed for these calls: {unobserved:?}");
    assert!(on_caller.is_empty(), "these calls parse JSON on the caller thread: {on_caller:?}");
}

static BLOCKING_RAN_ON: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<std::thread::ThreadId, std::thread::ThreadId>>> = std::sync::LazyLock::new(Default::default);

/// Records, per calling thread, the thread that ran the blocking work. Each test call runs on its own thread, so the entry belongs to that call.
pub(in super::super) fn note_blocking_thread(caller: std::thread::ThreadId) {
    BLOCKING_RAN_ON.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(caller, std::thread::current().id());
}

type FileCall = Box<dyn FnOnce(std::sync::Arc<super::Database>) -> std::pin::Pin<Box<dyn Future<Output = ()>>> + Send>;

fn file_calls() -> Vec<(&'static str, FileCall)> {
    let ids = || ("Moment".to_string(), "r1".to_string(), "photo".to_string());
    vec![
        ("attach", Box::new(move |db| Box::pin(async move { let (m, r, f) = ids(); let _ = db.attach(db.session_epoch(), m, r, f, crate::ffi::FileBytes { bytes: vec![1], name: "n".into(), mime: "image/png".into() }).await; }))),
        ("file", Box::new(move |db| Box::pin(async move { let (m, r, f) = ids(); let _ = db.file(db.session_epoch(), m, r, f).await; }))),
        ("read", Box::new(move |db| Box::pin(async move { let (m, r, f) = ids(); let _ = db.read(db.session_epoch(), m, r, f).await; }))),
        ("url", Box::new(move |db| Box::pin(async move { let (m, r, f) = ids(); let _ = db.url(db.session_epoch(), m, r, f).await; }))),
        ("detach", Box::new(move |db| Box::pin(async move { let (m, r, f) = ids(); let _ = db.detach(db.session_epoch(), m, r, f).await; }))),
    ]
}

#[tokio::test]
async fn calls_that_touch_files_run_their_work_off_the_caller_thread() {
    let (db, _) = rig(Ok("{}")).await;
    let mut on_caller = Vec::new();
    let mut unobserved = Vec::new();
    for (name, call) in file_calls() {
        let db = db.clone();
        let caller = join_within(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            runtime.block_on(call(db));
            std::thread::current().id()
        }));
        match BLOCKING_RAN_ON.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&caller) {
            None => unobserved.push(name),
            Some(ran_on) if ran_on == caller => on_caller.push(name),
            Some(_) => {}
        }
    }
    assert!(unobserved.is_empty(), "no blocking work was observed for these file calls: {unobserved:?}");
    assert!(on_caller.is_empty(), "these file calls run their work on the caller thread: {on_caller:?}");
}
