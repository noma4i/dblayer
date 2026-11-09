//! Owner-level tests of the `outbox` machine: the opening classes that classify a restored intent and defer its network to `RestoreReady`. Names are the `Test` column.

use super::outbox_cells_tests::{kinds, sends, transfer_outcomes};
use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use crate::owners::contract::Online;
use crate::owners::write::outbox::{CommitKind, NewOp, OutboxInput, OutboxOutcome, StoredOp};

fn stored(op_id: u64, row: &str) -> StoredOp {
    StoredOp { answered: None, op_id, op: model_op(row, Policy::Rollback, false), error_mark: false, pending: None, attempts: 1, cycle_id: 1, restore_failed: None, restore_element: None }
}

fn declare(twin: &mut Twin) { twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] }); }

fn restore(twin: &mut Twin, ops: Vec<StoredOp>) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Restored { ops, pending_lost: false }).into_iter().filter(|effect| !matches!(effect, OutboxEffect::RestoreClassified { .. })).collect()
}

fn ready(twin: &mut Twin) -> Vec<OutboxEffect> { twin.post(OutboxInput::RestoreReady { epoch: EPOCH }) }

#[test]
fn out_open_restores() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let restored = restore(&mut twin, vec![stored(7, "r")]);
    assert_eq!((twin.stage(7), kinds(&restored), sends(&restored)), (Some(St::PersistingAttempt), Vec::new(), 0), "online with a free row classifies to PersistingAttempt, no network before RestoreReady");
    let readied = ready(&mut twin);
    assert_eq!((kinds(&readied), sends(&readied)), (vec![CommitKind::Attempt], 0), "RestoreReady writes the attempt by seq, the request waits for its Committed");
}

#[test]
fn out_open_restores_failed() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    restore(&mut twin, vec![StoredOp { error_mark: true, ..stored(7, "r") }, stored(8, "r")]);
    let readied = ready(&mut twin);
    assert_eq!((twin.stage(7), twin.stage(8), sends(&readied)), (Some(St::Failed), Some(St::Queued), 0), "a marked intent is Failed, the later operation of the row waits behind it");
}

#[test]
fn out_open_restores_queued() {
    let mut offline = Twin::bare();
    declare(&mut offline);
    offline.post(OutboxInput::Online(Online(false)));
    let restored = restore(&mut offline, vec![stored(7, "r")]);
    let readied = ready(&mut offline);
    assert_eq!((offline.stage(7), restored.is_empty(), sends(&readied)), (Some(St::Queued), true, 0), "offline: the restored operation is Queued");

    let mut slot = Twin::bare();
    declare(&mut slot);
    restore(&mut slot, vec![stored(7, "a"), stored(8, "b")]);
    ready(&mut slot);
    assert_eq!((slot.stage(7), slot.stage(8)), (Some(St::PersistingAttempt), Some(St::Queued)), "the lowest seq takes the one slot, the next is Queued");
}

#[test]
fn out_open_restores_transfer() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let restored = restore(&mut twin, vec![StoredOp { op: model_op("r", Policy::Rollback, true), ..stored(7, "r") }]);
    assert_eq!((twin.stage(7), sends(&restored)), (Some(St::PersistingAttempt), 0), "a restored transfer operation classifies with no network before RestoreReady");
    ready(&mut twin);
    let started = twin.committed(7);
    assert_eq!((twin.stage(7), sends(&started)), (Some(St::Sending), 1), "the deferred network effect goes after RestoreReady");
    twin.post(OutboxInput::Response { op_id: 7, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let landed = twin.committed(7);
    assert!(matches!(transfer_outcomes(&landed).as_slice(), [OutboxOutcome::Confirmed { .. }]), "the outcome of the restored transfer operation goes to Transfer: {landed:?}");
}

#[test]
fn out_open_format_change() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let after_format = StoredOp { op: NewOp { action: "update".into(), effects: Default::default(), ..model_op("r", Policy::Rollback, false) }, ..stored(7, "r") };
    let restored = restore(&mut twin, vec![after_format]);
    assert_eq!((twin.stage(7), sends(&restored)), (Some(St::PersistingAttempt), 0), "the saved intent classifies after a format change, no network before RestoreReady");
    let readied = ready(&mut twin);
    assert!(!twin.store_text.contains("optimistic"), "the optimistic row of a saved intent is not restored: {}", twin.store_text);
    assert_eq!((kinds(&readied), sends(&readied)), (vec![CommitKind::Attempt], 0));
    twin.committed(7);
    twin.post(OutboxInput::Response { op_id: 7, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert!(twin.store_text.contains("server"), "the row comes from the server answer: {}", twin.store_text);
}
