use std::sync::{mpsc, Arc};
use std::time::Duration;

use super::fixtures::*;
use super::*;
use crate::test_wait::join_within;

#[test]
fn inv_conc_07_disk_read_finishes_while_writer_has_uncommitted_changes() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(&directory.path().join("reads.sqlite")).unwrap());
    let mut presence = items_spec();
    presence.storage = crate::schema::Storage::Memory;
    store.boot(&[messages_spec(), presence], None).unwrap();
    store.commit(envelope(&store, vec![upsert("messages", message("m1", "before"))], vec![])).unwrap();
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
                released.recv_timeout(Duration::from_secs(10)).unwrap();
                Ok(())
            }).unwrap();
        }));
    }
    let results: Vec<_> = (0..2).map(|_| received.recv_timeout(Duration::from_secs(2))).collect();
    for release in releases {
        release.send(()).unwrap();
    }
    transaction.rollback().unwrap();
    drop(writer);
    for thread in threads {
        join_within(thread);
    }
    for result in results {
        assert_eq!(result.unwrap(), serde_json::json!("before"));
    }
}
