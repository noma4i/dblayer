use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::db::fixtures::*;
use crate::db::open::locked;
use crate::query::Query;
use crate::test_wait::join_within;

#[test]
fn inv_ephemeral_09_a_read_of_two_models_shows_one_snapshot() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.await_revision(db.revision_seq().unwrap());
    locked(&db.shared.cache).evict("moments", "m1");
    locked(&db.shared.cache).evict("users", "u1");
    let fired = AtomicBool::new(false);
    let writer = db.clone();
    let write = Arc::new(std::sync::Mutex::new(None));
    let slot = write.clone();
    *locked(&db.shared.row_build_hook) = Some(Arc::new(move |id: &str| {
        if id == "m1" && !fired.swap(true, Ordering::SeqCst) {
            let writer = writer.clone();
            let (done, finished) = std::sync::mpsc::channel();
            *slot.lock().unwrap() = Some(std::thread::spawn(move || {
                writer.update(&writer.admit().unwrap(), "users", &rid("u1"), obj(json!({"name": "Bea"}))).unwrap();
                let _ = done.send(());
            }));
            let _ = finished.recv_timeout(Duration::from_millis(300));
        }
    }));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").including(&["author"]), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    join_within(write.lock().unwrap().take().expect("the hook started the write"));
    db.await_revision(db.revision_seq().unwrap());
    let seen = changes(&log);
    let first = &seen[0];
    assert_eq!(first.related["author"][&rid("m1")][0].text("name"), Some("Ann"), "the included row came from a later snapshot than the row that includes it");
    let last = seen.last().unwrap();
    assert_eq!(last.related["author"][&rid("m1")][0].text("name"), Some("Bea"), "the commit made between the two reads is delivered afterwards");
}
