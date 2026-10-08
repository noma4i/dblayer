use std::sync::Arc;

use crate::db::listeners::Mailbox;
use crate::db::watch::{Body, RowsBody, Version};
use crate::identity::RowId;

fn version(revision: i64) -> Arc<Version> {
    Arc::new(Version { revision, epoch: 0, body: Body::Rows(RowsBody { entries: Vec::new(), rows: Vec::new(), related: Default::default(), step: None }) })
}

fn id(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

#[test]
fn inv_observe_12_a_chain_of_replaced_pairs_collapses_to_one_pair() {
    let mut mailbox = Mailbox::default();
    mailbox.push(version(1), &[(id("temp-a"), id("temp-b"))]);
    mailbox.push(version(2), &[(id("temp-b"), id("m1"))]);
    mailbox.push(version(3), &[(id("temp-x"), id("m2"))]);
    let taken = mailbox.take().unwrap();
    assert_eq!(taken.replaced, vec![(id("temp-a"), id("m1")), (id("temp-x"), id("m2"))]);
}

#[test]
fn inv_observe_12_replaced_pairs_of_one_observer_are_capped_and_the_overflow_asks_for_a_full_recount() {
    let mut mailbox = Mailbox::default();
    for turn in 0..12_000 {
        mailbox.push(version(turn + 1), &[(id(&format!("temp-{turn}")), id(&format!("m{turn}")))]);
    }
    let taken = mailbox.take().unwrap();
    assert!(taken.replaced.len() <= 10_000, "{} pairs are held", taken.replaced.len());
    assert!(taken.overflowed);
}
