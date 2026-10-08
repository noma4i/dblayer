use super::listeners::Mailbox;
use super::watch::{Body, RowsBody, Version};
use crate::identity::RowId;
use std::sync::Arc;

fn version(revision: i64) -> Arc<Version> {
    Arc::new(Version { revision, epoch: 0, body: Body::Rows(RowsBody { entries: Vec::new(), rows: Vec::new(), related: Default::default(), step: None }) })
}

#[test]
fn a_mailbox_keeps_only_the_latest_version_and_all_replaced_pairs() {
    let mut mailbox = Mailbox::default();
    let pair = (RowId::new("temp-1").unwrap(), RowId::new("m1").unwrap());
    assert!(mailbox.push(version(1), &[]));
    assert!(!mailbox.push(version(2), std::slice::from_ref(&pair)));
    assert!(!mailbox.push(version(3), &[]));
    let taken = mailbox.take().unwrap();
    assert_eq!(taken.latest.revision, 3);
    assert_eq!(taken.steps, 3);
    assert_eq!(taken.replaced, vec![pair]);
    assert!(mailbox.take().is_none());
}

#[test]
fn a_mailbox_asks_for_a_new_run_only_when_it_is_not_scheduled() {
    let mut mailbox = Mailbox::default();
    assert!(mailbox.push(version(1), &[]));
    mailbox.take().unwrap();
    assert!(!mailbox.push(version(2), &[]));
    mailbox.finish();
    assert!(mailbox.push(version(3), &[]));
}
