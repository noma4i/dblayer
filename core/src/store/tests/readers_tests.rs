use std::sync::{mpsc, Arc};

use super::fixtures::*;
use super::*;
use crate::test_wait::{join_within, recv_within};

#[test]
fn store_read_snapshot_waits_for_commit_journal() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(&directory.path().join("journal.sqlite"), Arc::new(crate::runtime::SystemClock)).unwrap());
    store.boot(&[messages_spec()]).unwrap();
    store.mark_ready();
    let before = store.revision_seq().unwrap();
    store.context.changed.start(before);
    let (sent, reached) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    store.set_commit_log(Box::new(move |_| { sent.send(()).unwrap(); recv_within(&released.lock().unwrap()); })).unwrap();
    let writer = store.clone();
    let thread = std::thread::spawn(move || writer.commit(envelope(vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap());
    recv_within(&reached);
    let (seen, present) = store.read(&["messages"], |reader| Ok((reader.revision_seq()?, reader.stored_row("messages", "m1")?.is_some()))).unwrap();
    let changed = store.context.changed.changed_between((before, 0), (seen, 0), 100);
    release.send(()).unwrap();
    join_within(thread);
    assert!(present && seen > before);
    assert!(changed.is_some_and(|rows| rows.contains(&("row_messages".into(), "m1".into()))), "a visible row has its commit in the journal");
}

#[test]
fn inv_conc_07_disk_read_finishes_while_writer_has_uncommitted_changes() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(&directory.path().join("reads.sqlite"), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    let mut presence = items_spec();
    presence.storage = crate::schema::Storage::Memory;
    store.boot(&[messages_spec(), presence]).unwrap();
    store.commit(envelope(vec![upsert("messages", message("m1", "before"))], vec![])).unwrap();
    let writer = store.lock();
    let transaction = writer.connection.unchecked_transaction().unwrap();
    transaction.execute("DELETE FROM row_messages WHERE id = 'm1'", []).unwrap();
    let (sent, received) = mpsc::channel();
    let mut threads = Vec::new();
    let mut releases = Vec::new();
    for _ in 0..2 {
        let reading = store.clone();
        let sent = sent.clone();
        let (release, released) = mpsc::channel();
        releases.push(release);
        threads.push(std::thread::spawn(move || {
            reading.read(&["messages"], |reader| {
                let row = reader.stored_row("messages", "m1")?.unwrap();
                sent.send(row.row["chatId"].clone()).unwrap();
                recv_within(&released);
                Ok(())
            }).unwrap();
        }));
    }
    let results: Vec<_> = (0..2).map(|_| recv_within(&received)).collect();
    for release in releases {
        release.send(()).unwrap();
    }
    transaction.rollback().unwrap();
    drop(writer);
    for thread in threads {
        join_within(thread);
    }
    for result in results {
        assert_eq!(result, serde_json::json!("before"));
    }
}
