use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use super::failure_tests::rig;

fn ready_on_first_poll<T>(call: impl Future<Output = T>) -> bool {
    let mut call = pin!(call);
    matches!(call.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(_))
}

#[tokio::test]
async fn calls_with_json_arguments_parse_them_off_the_caller_thread() {
    let (db, _) = rig(Ok("{}")).await;
    let bad = || "{".to_string();
    let (moment, row) = (|| "Moment".to_string(), || "r1".to_string());
    let calls: Vec<(&str, bool)> = vec![
        ("upload", ready_on_first_poll(db.upload(moment(), row(), "upload".into(), bad()))),
        ("upload_create", ready_on_first_poll(db.upload_create(moment(), "upload".into(), bad(), bad()))),
        ("upload_create_batch", ready_on_first_poll(db.upload_create_batch(moment(), bad(), bad()))),
        ("run", ready_on_first_poll(db.run("Feed".into(), bad()))),
        ("load", ready_on_first_poll(db.load(moment(), "feed".into(), bad()))),
        ("refresh", ready_on_first_poll(db.refresh(moment(), "feed".into(), bad()))),
        ("load_more", ready_on_first_poll(db.load_more(moment(), "feed".into(), bad()))),
        ("warm", ready_on_first_poll(db.warm(bad()))),
    ];
    let on_caller: Vec<&str> = calls.iter().filter(|(_, ready)| *ready).map(|(name, _)| *name).collect();
    assert!(on_caller.is_empty(), "these calls parse JSON before leaving the caller thread: {on_caller:?}");
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
