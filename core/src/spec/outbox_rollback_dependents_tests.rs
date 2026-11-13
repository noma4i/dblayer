//! Owner-level test of P-OUT-58: the rollback batch of a `create` that ends its dependents is repeated with them after a Storage refusal.

use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{OutboxAnswer, OutboxInput, OutboxOutcome};

const TEMP: &str = "temp-0F2B7C1E-5D3A-4C8B-9E10-7A6B5C4D3E2F";
const CREATE_WAITER: u64 = 910;
const DEPENDENT_WAITER: u64 = 911;

fn submit_row(twin: &mut Twin, action: &str) -> crate::owners::write::outbox::OpId {
    twin.submit_as(TEMP, Policy::Rollback, false, action);
    *twin.state().ops.keys().max().expect("the operation exists")
}

fn rollback_ops(ops: &[StoreOp]) -> Vec<u64> {
    ops.iter().filter_map(|op| match op { StoreOp::RollBack { op_id } => Some(*op_id), _ => None }).collect()
}

fn outcomes(effects: &[OutboxEffect], reply: u64) -> Vec<OutboxOutcome> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Reply { reply: found, answer: OutboxAnswer::Outcome(outcome) } if *found == reply => Some(outcome.clone()), _ => None }).collect()
}

#[test]
fn out_rollback_retry_keeps_the_dependents() {
    let mut twin = Twin::new();
    let create = submit_row(&mut twin, "create");
    twin.committed(create);
    let dependent = submit_row(&mut twin, "update");
    twin.committed(dependent);
    assert_eq!((twin.stage(create), twin.stage(dependent)), (Some(St::Sending), Some(St::Queued)));
    let (create_receipt, dependent_receipt) = (twin.receipt(create, EPOCH), twin.receipt(dependent, EPOCH));
    twin.post(OutboxInput::Outcome { receipt: create_receipt, reply: CREATE_WAITER, discard_on_failure: false });
    twin.post(OutboxInput::Outcome { receipt: dependent_receipt, reply: DEPENDENT_WAITER, discard_on_failure: false });

    let first = twin.post(OutboxInput::TransportError { op_id: create, temporary: false });
    let first_ops = rollback_ops(&twin.store_ops);
    assert_eq!(first_ops.len(), 2, "the first rollback batch carries the create and its dependent: {:?}", twin.store_ops);
    let first_batch = twin.batches[&create];
    let refused = twin.reject_batch(first_batch, RReason::Storage);
    let due = super::outbox_cells_tests::tick(&mut twin, create);
    let second_ops = rollback_ops(&twin.store_ops);
    assert_eq!(second_ops, first_ops, "the repeated rollback batch carries the same operations: {:?}", twin.store_ops);
    let second_batch = twin.stores.iter().map(|(id, _)| *id).max().expect("the repeated batch");
    let done = twin.commit_batch(second_batch);

    let mut effects = first;
    effects.extend(refused);
    effects.extend(due);
    effects.extend(done);
    assert!(twin.state().ops.values().all(|operation| operation.stage == crate::owners::write::outbox::Stage::Done) || twin.state().ops.is_empty(), "no operation is left in the queue: {:?}", twin.state().ops.keys().collect::<Vec<_>>());
    let create_ends = outcomes(&effects, CREATE_WAITER);
    let dependent_ends = outcomes(&effects, DEPENDENT_WAITER);
    assert!(matches!(create_ends.as_slice(), [OutboxOutcome::Rejected { .. }]), "{create_ends:?}");
    assert!(matches!(dependent_ends.as_slice(), [OutboxOutcome::Rejected { .. }]), "{dependent_ends:?}");
}
