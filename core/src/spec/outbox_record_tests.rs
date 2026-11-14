//! Owner-level tests of the `outbox` machine: the record of the outcome of an operation (D-466).

use super::outbox_harness::*;
use super::outbox_harness::St;
use crate::owners::store::{OutcomeOwner, StoreOp};
use crate::owners::write::outbox::{OutboxEffect as Run, OutboxInput, OutboxOutcome};

/// The terminal batch of a landing writes the record with the intent removal, and after its Committed the owner keeps nothing for the operation.
#[test]
fn out_done_keeps_nothing_after_the_record_committed() {
    let (twin, id) = reach(St::Done, &ready());
    assert!(twin.state().ops.is_empty(), "the finished operation left the owner: {:?}", twin.state().ops.keys().collect::<Vec<_>>());
    assert!(twin.sent_ops().iter().any(|op| matches!(op, StoreOp::Outcome { owner: OutcomeOwner::Operation, id: written, outcome: OutboxOutcome::Confirmed { row_id: Some(_), .. }, .. } if *written == id)), "{:?}", twin.sent_ops());
}

/// A receipt of an operation that left the memory is answered from the record of the Store.
#[test]
fn out_outcome_after_done_reads_the_record() {
    let (mut twin, id) = reach(St::Done, &ready());
    twin.history();
    twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH), reply: 91, discard_on_failure: false });
    let reads: Vec<_> = twin.history().into_iter().filter_map(|seen| match seen { Seen::Run(Run::ReadOutcome { reply, op_id, cycle_id }) => Some((reply, op_id, cycle_id)), _ => None }).collect();
    assert_eq!(reads, vec![(91, id, 1)]);
}
