use super::listeners::Mailbox;
use super::watch::{Body, Loaded, RowsBody, Version};
use crate::identity::RowId;
use std::sync::Arc;

fn version(revision: i64) -> Arc<Version> {
    Arc::new(Version { revision, epoch: 0, body: Body::Rows(RowsBody::new(Vec::new())) })
}

#[test]
fn a_mailbox_keeps_only_the_latest_version_and_all_replaced_pairs() {
    let mut mailbox = Mailbox::default();
    let pair = (RowId::new("temp-1").unwrap(), RowId::new("m1").unwrap());
    assert!(mailbox.push(version(1), &[], Loaded::default()));
    assert!(!mailbox.push(version(2), std::slice::from_ref(&pair), Loaded::default()));
    assert!(!mailbox.push(version(3), &[], Loaded::default()));
    let taken = mailbox.take().unwrap();
    assert_eq!(taken.latest.revision, 3);
    assert_eq!(taken.steps, 3);
    assert_eq!(taken.replaced, vec![pair]);
    assert!(mailbox.take().is_none());
}

#[test]
fn a_mailbox_asks_for_a_new_run_only_when_it_is_not_scheduled() {
    let mut mailbox = Mailbox::default();
    assert!(mailbox.push(version(1), &[], Loaded::default()));
    mailbox.take().unwrap();
    assert!(!mailbox.push(version(2), &[], Loaded::default()));
    mailbox.finish();
    assert!(mailbox.push(version(3), &[], Loaded::default()));
}

#[test]
fn inv_panic_01_a_listener_worker_that_cannot_start_still_delivers_and_releases_the_busy_count() {
    use super::fixtures::{booted, calls, recorder};
    use crate::query::Query;
    use std::sync::atomic::Ordering;
    let db = booted();
    db.shared.executor.refuse_spawn.store(true, Ordering::SeqCst);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log), vec![0]);
}

#[test]
fn a_paused_mailbox_merges_every_push_and_owes_exactly_one_run() {
    let mut mailbox = Mailbox::default();
    mailbox.pause();
    for revision in 1..=5 {
        assert!(!mailbox.push(version(revision), &[], Loaded::default()), "no run is asked for while paused");
    }
    assert!(mailbox.resume(), "resume asks for the owed run");
    assert!(!mailbox.resume(), "and only once");
    let taken = mailbox.take().unwrap();
    assert_eq!((taken.latest.revision, taken.steps), (5, 5), "one delivery with the latest version");
    assert!(taken.delivered.is_none());
}

#[test]
fn a_paused_mailbox_with_nothing_owed_asks_for_no_run_on_resume() {
    let mut mailbox = Mailbox::default();
    mailbox.pause();
    assert!(!mailbox.resume());
}

#[test]
fn cancelling_while_paused_leaves_nothing_owed() {
    let mut mailbox = Mailbox::default();
    mailbox.pause();
    mailbox.push(version(1), &[], Loaded::default());
    mailbox.forget_owed();
    assert!(!mailbox.resume());
    assert!(mailbox.take().is_none());
}
