use std::sync::atomic::{AtomicUsize, Ordering};
use serde_json::json;

use super::super::rig::*;
use super::rig::*;
use crate::db::Db;
use crate::transport::Reply;

const SMALL: usize = 200;
const LARGE: usize = 2000;
const FIRST_SEQUENCE: i64 = 1000;
const WINDOW: usize = 20;

static STEPS: AtomicUsize = AtomicUsize::new(0);

struct Work {
    steps: usize,
    rows_loaded: usize,
    _directory: tempfile::TempDir,
}

fn chat_in_file() -> (Rig, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let rig = chat_rig_on(Db::open(&directory.path().join("chat.sqlite"), vec![messages_spec(true)], None).unwrap());
    (rig, directory)
}

fn enqueue_chat(rig: &Rig, size: usize) {
    let nodes = (0..size).map(|index| message(&format!("m{index:05}"), Some(FIRST_SEQUENCE + index as i64), (index % 60) as u32)).collect();
    enqueue_page(rig, nodes, true, "c");
}

async fn measured<F: std::future::Future<Output = ()>>(rig: &Rig, directory: tempfile::TempDir, operation: F) -> Work {
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    STEPS.store(0, Ordering::SeqCst);
    {
        let inner = rig.db.shared.store.lock();
        inner.connection.progress_handler(1, Some(|| STEPS.fetch_add(1, Ordering::SeqCst) == usize::MAX)).unwrap();
    }
    let loaded = rig.db.counters().rows_loaded;
    operation.await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    {
        let inner = rig.db.shared.store.lock();
        inner.connection.progress_handler(0, None::<fn() -> bool>).unwrap();
    }
    let steps = STEPS.load(Ordering::SeqCst);
    Work { steps, rows_loaded: rig.db.counters().rows_loaded - loaded, _directory: directory }
}

async fn event_row(size: usize) -> Work {
    let (rig, directory) = chat_in_file();
    enqueue_chat(&rig, size);
    load_first_page(&rig).await;
    let _subscription = subscribe_to_events(&rig).await;
    let (_log, _window) = watch_thread(&rig, WINDOW);
    measured(&rig, directory, event(&rig, message("new", Some(FIRST_SEQUENCE + size as i64), 1))).await
}

async fn confirmed_row(size: usize) -> Work {
    let (rig, directory) = chat_in_file();
    enqueue_chat(&rig, size);
    load_first_page(&rig).await;
    let (_log, _window) = watch_thread(&rig, WINDOW);
    rig.db.as_app().create("messages", params(json!({"chatId": CHAT, "text": "mine", "createdAt": "2026-01-01T00:00:09.000Z"}))).unwrap();
    rig.transport.enqueue("CreateMessage", Reply::data(json!({"createMessage": message("new", Some(FIRST_SEQUENCE + size as i64), 9)})));
    measured(&rig, directory, async { crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap(); }).await
}

async fn older_page(size: usize) -> Work {
    let (rig, directory) = chat_in_file();
    enqueue_chat(&rig, size);
    let known = (0..10).map(|index| message(&format!("m{index:05}"), Some(FIRST_SEQUENCE + index as i64), (index % 60) as u32));
    enqueue_page(&rig, (1..=30).map(|index| message(&format!("old{index:02}"), Some(index), 1)).chain(known).collect(), false, "c2");
    load_first_page(&rig).await;
    let (_log, _window) = watch_thread(&rig, WINDOW);
    measured(&rig, directory, load_next_page(&rig)).await
}

#[tokio::test(start_paused = true)]
async fn one_change_costs_the_same_work_at_every_list_size() {
    for (name, small, large) in [
        ("an event row", event_row(SMALL).await, event_row(LARGE).await),
        ("a confirmed row", confirmed_row(SMALL).await, confirmed_row(LARGE).await),
        ("an older page that repeats shown rows", older_page(SMALL).await, older_page(LARGE).await),
    ] {
        assert!(large.steps <= small.steps + small.steps / 10, "{name}: {} write steps at {SMALL} rows, {} at {LARGE}", small.steps, large.steps);
        assert!(small.rows_loaded <= WINDOW && large.rows_loaded <= WINDOW, "{name}: rows read {} at {SMALL} rows, {} at {LARGE}, window of {WINDOW}", small.rows_loaded, large.rows_loaded);
    }
}
