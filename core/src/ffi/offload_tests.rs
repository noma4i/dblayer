use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use super::failure_tests::rig;
use super::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};

fn ready_on_first_poll<T>(call: impl Future<Output = T>) -> bool {
    let mut call = pin!(call);
    matches!(call.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(_))
}

const MARKER_COLUMN: usize = 10_000;

static PARSE_FAILED_ON: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<usize, std::thread::ThreadId>>> = std::sync::LazyLock::new(Default::default);

/// Records the thread that turned a marked parse failure into a `DbError`. The call marks its failure with a column number of at least `MARKER_COLUMN`, so other tests never write here.
pub(super) fn note_parse_failure(error: &serde_json::Error) {
    if error.column() >= MARKER_COLUMN {
        PARSE_FAILED_ON.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(error.column(), std::thread::current().id());
    }
}

type Call = Box<dyn FnOnce(std::sync::Arc<super::Database>, String) -> std::pin::Pin<Box<dyn Future<Output = Result<(), super::DbError>>>> + Send>;

fn request(variables_json: String) -> WireUploadRequest {
    WireUploadRequest { path: "p".into(), name: None, mime: None, variables_json, create_blob: None, multipart: None }
}

fn json_calls() -> Vec<(&'static str, Call)> {
    let moment = || "Moment".to_string();
    vec![
        ("upload", Box::new(move |db, bad| Box::pin(async move { db.upload(moment(), "r1".into(), "upload".into(), request(bad)).await }))),
        ("upload_create", Box::new(move |db, bad| Box::pin(async move { db.upload_create(moment(), "upload".into(), bad, request("{}".into())).await.map(drop) }))),
        ("upload_create_batch", Box::new(move |db, bad| {
            Box::pin(async move {
                let item = WireUploadItem { values_json: bad, path: "p".into(), name: None, mime: None, variables_json: "{}".into() };
                let batch = WireUploadBatch { operation: "o".into(), variables_json: "{}".into(), create_blob: None, multipart: None };
                db.upload_create_batch(moment(), vec![item], batch).await.map(drop)
            })
        })),
        ("run", Box::new(move |db, bad| Box::pin(async move { db.run("Feed".into(), bad).await.map(drop) }))),
        ("load", Box::new(move |db, bad| Box::pin(async move { db.load(moment(), "feed".into(), bad).await }))),
        ("refresh", Box::new(move |db, bad| Box::pin(async move { db.refresh(moment(), "feed".into(), bad).await }))),
        ("load_more", Box::new(move |db, bad| Box::pin(async move { db.load_more(moment(), "feed".into(), bad).await.map(drop) }))),
        ("warm", Box::new(move |db, bad| Box::pin(async move { db.warm(bad).await }))),
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
        let (caller, outcome) = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let outcome = runtime.block_on(call(db, bad));
            (std::thread::current().id(), outcome)
        })
        .join()
        .unwrap();
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

#[tokio::test]
async fn calls_that_touch_files_leave_the_caller_thread_before_any_work() {
    let (db, _) = rig(Ok("{}")).await;
    let ids = || ("Moment".to_string(), "r1".to_string(), "photo".to_string());
    let calls: Vec<(&str, bool)> = vec![
        ("attach", ready_on_first_poll({ let (m, r, f) = ids(); db.attach(m, r, f, vec![1], "n".into(), "image/png".into()) })),
        ("file", ready_on_first_poll({ let (m, r, f) = ids(); db.file(m, r, f) })),
        ("read", ready_on_first_poll({ let (m, r, f) = ids(); db.read(m, r, f) })),
        ("url", ready_on_first_poll({ let (m, r, f) = ids(); db.url(m, r, f) })),
        ("detach", ready_on_first_poll({ let (m, r, f) = ids(); db.detach(m, r, f) })),
        ("maintain", ready_on_first_poll(db.maintain())),
    ];
    let on_caller: Vec<&str> = calls.iter().filter(|(_, ready)| *ready).map(|(name, _)| *name).collect();
    assert!(on_caller.is_empty(), "these file calls run on the caller thread: {on_caller:?}");
}
