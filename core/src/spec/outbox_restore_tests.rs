//! Owner-level tests of the `outbox` machine: the phase `Restoring` with its held calls, the classes of the opening and the declarations. Names are the `Test` column.

use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use crate::owners::contract::{Committed, EpochStarted, Online, RejectReason, Rejected};
use crate::owners::session::StoredAnswer;
use crate::owners::write::outbox::{CommitKind, Fault, OutboxAnswer, OutboxInput, OutboxOutcome, Phase, Receipt, StoredOp};
use crate::runtime::Cause;

pub(super) fn stored(op_id: u64, row: &str) -> StoredOp {
    StoredOp { answered: None, op_id, op: model_op(row, Policy::Rollback, false), created_at: 0, error_mark: false, pending: None, attempts: 1, cycle_id: 1, cause: None, restore_failed: None, restore_element: None, removed_rows: Vec::new() }
}

#[test]
fn out_restore_discards_only_orphaned_transfer_intents() {
    use crate::owners::write::outbox::OpSource;
    let mut twin = Twin::bare();
    declare(&mut twin);
    twin.post(OutboxInput::OrphanTransfers { op_ids: vec![1] });
    let mut orphan = stored(1, "r");
    orphan.error_mark = true;
    orphan.op.origin = OpSource::Transfer { transfer_id: 1, cycle_id: 1 };
    let mut live = stored(2, "other");
    live.error_mark = true;
    live.op.origin = OpSource::Transfer { transfer_id: 2, cycle_id: 1 };
    restore(&mut twin, vec![orphan, live]);
    let effects = ready(&mut twin);
    assert_eq!(kinds(&effects), vec![CommitKind::Rollback], "{effects:?}");
    assert_eq!(twin.stage(2), Some(St::Failed));
    assert!(twin.store_text.contains("RetireTransferOutcome") && twin.store_text.contains("Discarded"), "{}", twin.store_text);
}

pub(super) fn declare(twin: &mut Twin) { twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] }); }

pub(super) fn restore(twin: &mut Twin, ops: Vec<StoredOp>) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Restored { ops, pending_lost: false }).into_iter().filter(|effect| !matches!(effect, OutboxEffect::RestoreClassified { .. })).collect()
}

pub(super) fn ready(twin: &mut Twin) -> Vec<OutboxEffect> { twin.post(OutboxInput::RestoreReady { epoch: EPOCH }) }

fn classified(effects: &[OutboxEffect]) -> Vec<u64> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::RestoreClassified { epoch } => Some(*epoch), _ => None }).collect()
}

fn kinds(effects: &[OutboxEffect]) -> Vec<CommitKind> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Store { kind, .. } => Some(*kind), _ => None }).collect()
}

fn sends(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Send { .. })).count() }

fn answers(effects: &[OutboxEffect], reply: u64) -> Vec<OutboxAnswer> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Reply { reply: found, answer } if *found == reply => Some(answer.clone()), _ => None }).collect()
}

fn drained(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count() }

fn submit(twin: &mut Twin, reply: u64) -> Vec<OutboxEffect> { twin.post(OutboxInput::Submit { epoch: EPOCH, op: model_op("r", Policy::Rollback, false), reply }) }

fn call_inputs() -> Vec<OutboxInput> {
    vec![
        OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 41 },
        OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 42 },
        OutboxInput::Outcome { receipt: Receipt { epoch: EPOCH, op_id: 99, cycle_id: 1, revision: 0, optimistic: None }, reply: 43 , discard_on_failure: false},
    ]
}

#[test]
fn out_submit_while_restoring() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let held = submit(&mut twin, 40);
    assert!(kinds(&held).is_empty() && answers(&held, 40).is_empty() && twin.state().ops.is_empty(), "nothing is written before Restored: {held:?}");
    let restored = restore(&mut twin, Vec::new());
    assert_eq!(kinds(&restored), vec![CommitKind::Intent], "the held Submit is written after Restored");
}

#[test]
fn out_restoring_holds_calls() {
    let mut twin = Twin::bare();
    for input in call_inputs() {
        assert!(twin.post(input).is_empty());
    }
    let restored = restore(&mut twin, Vec::new());
    assert_eq!(answers(&restored, 41), vec![OutboxAnswer::Err(Fault::NotFound)]);
    assert_eq!(answers(&restored, 42), vec![OutboxAnswer::Err(Fault::NotFound)]);
    assert_eq!(answers(&restored, 43), vec![OutboxAnswer::Err(Fault::NotFound)]);
}

#[test]
fn out_restoring_online() {
    let mut twin = Twin::bare();
    assert!(twin.post(OutboxInput::Online(Online(false))).is_empty());
    assert!(!twin.state().online);
}

#[test]
fn out_restoring_epoch_ended() {
    let mut twin = Twin::bare();
    submit(&mut twin, 40);
    for input in call_inputs() {
        twin.post(input);
    }
    take_fx(&twin.log, None);
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    let ended = take_fx(&twin.log, state_of(&twin.det));
    for reply in [40, 41, 42] {
        assert_eq!(answers(&ended, reply), vec![OutboxAnswer::Err(Fault::SessionEnded)], "reply {reply}");
    }
    assert_eq!(answers(&ended, 43), vec![OutboxAnswer::Outcome(OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded))]);
    let late = twin.post(OutboxInput::Restored { ops: vec![stored(7, "r")], pending_lost: false });
    assert!(late.is_empty(), "a late Restored of the ended epoch is a foreign input and tells nobody: {late:?}");
}

#[test]
fn out_restoring_foreign() {
    let mut twin = Twin::bare();
    let inputs = vec![
        OutboxInput::RetryDue { op_id: 1, cycle_id: 1 },
        OutboxInput::Committed(Committed { batch: 1, revision: 1, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }),
        OutboxInput::Rejected(Rejected { batch: 1, reason: RejectReason::Storage("disk".into()) }),
        OutboxInput::Response { op_id: 1, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: 1, temporary: true },
        OutboxInput::Dropped { op_id: 1 },
    ];
    for input in inputs {
        let effects = twin.post(input.clone());
        assert!(effects.is_empty(), "{input:?}: {effects:?}");
    }
}

#[test]
fn out_epoch_started_tells_lifecycle() {
    let mut twin = Twin::bare();
    let effects = twin.post(OutboxInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(9), generation: 2 }));
    assert!(effects.iter().any(|effect| matches!(effect, OutboxEffect::Started { epoch } if *epoch == EPOCH)), "Started names the epoch: {effects:?}");
}

#[test]
fn out_restored() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let effects = twin.post(OutboxInput::Restored { ops: vec![stored(7, "r")], pending_lost: false });
    assert_eq!((twin.state().phase, twin.stage(7), sends(&effects), kinds(&effects).len()), (Phase::Classified, Some(St::Queued), 0, 0), "{effects:?}");
    assert_eq!(classified(&effects), vec![EPOCH], "{effects:?}");
}

#[test]
fn out_restoring_closing() {
    let mut twin = Twin::bare();
    submit(&mut twin, 40);
    for input in call_inputs() {
        twin.post(input);
    }
    take_fx(&twin.log, None);
    twin.det.close();
    twin.run();
    let closed = take_fx(&twin.log, state_of(&twin.det));
    assert_eq!(answers(&closed, 40), vec![OutboxAnswer::Err(Fault::Closed)]);
    assert_eq!(answers(&closed, 43), vec![OutboxAnswer::Outcome(OutboxOutcome::Ended(crate::ffi::EndCause::Closed))]);
    assert_eq!(drained(&closed), 1);
}

#[test]
fn out_restore_ready_other() {
    let mut twin = Twin::new();
    assert!(ready(&mut twin).is_empty());
    let mut classified = Twin::bare();
    declare(&mut classified);
    restore(&mut classified, vec![stored(7, "r")]);
    assert!(classified.post(OutboxInput::RestoreReady { epoch: EPOCH + 1 }).is_empty());
    assert_eq!(classified.state().phase, Phase::Classified);
}

#[test]
fn out_register_rechecks_waiting() {
    let mut twin = Twin::bare();
    restore(&mut twin, vec![stored(7, "r")]);
    assert_eq!(twin.stage(7), Some(St::WaitingDeclaration));
    ready(&mut twin);
    let effects = twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
    assert_eq!((twin.stage(7), kinds(&effects)), (Some(St::Sending), vec![]));
    assert_eq!(sends(&effects), 1);
}

#[test]
fn out_waiting_declaration_refuses() {
    let mut twin = Twin::bare();
    restore(&mut twin, vec![stored(7, "r")]);
    let retry = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 41 });
    let discard = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 42 });
    assert_eq!((answers(&retry, 41), answers(&discard, 42)), (vec![OutboxAnswer::Err(Fault::InFlight)], vec![OutboxAnswer::Err(Fault::InFlight)]));
}

#[test]
fn out_open_queue_unreadable() {
    let mut twin = Twin::bare();
    let effects = twin.post(OutboxInput::RestoreFailed { error: "disk".into() });
    assert_eq!(classified(&effects), vec![EPOCH], "{effects:?}");
    assert!(effects.len() == 1 && twin.state().ops.is_empty());
    assert_eq!(twin.state().phase, Phase::Classified);
}

#[test]
fn out_queue_failed_refuses() {
    let mut twin = Twin::bare();
    twin.post(OutboxInput::RestoreFailed { error: "disk".into() });
    ready(&mut twin);
    let storage = Fault::Rejected(RejectReason::Storage("disk".into()));
    let effects = submit(&mut twin, 40);
    assert_eq!(answers(&effects, 40), vec![OutboxAnswer::Err(storage.clone())]);
    for input in call_inputs().into_iter().take(2) {
        let reply = match &input { OutboxInput::Retry { reply, .. } | OutboxInput::Discard { reply, .. } => *reply, _ => unreachable!() };
        assert_eq!(answers(&twin.post(input), reply), vec![OutboxAnswer::Err(storage.clone())]);
    }
}

#[test]
fn out_open_restores_answered() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let blocker = stored(6, "r");
    let answered = StoredOp { answered: Some((41, StoredAnswer::Ok("{}".into()))), ..stored(7, "r") };
    let effects = restore(&mut twin, vec![blocker, answered]);
    assert_eq!(twin.stage(7), Some(St::Landing), "the row is not awaited: the server applied the operation");
    assert!(kinds(&effects).is_empty() && sends(&effects) == 0, "the rows are read first: {effects:?}");
    let landed = twin.post(OutboxInput::Response { op_id: 7, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert!(kinds(&landed).contains(&CommitKind::Land) && sends(&landed) == 0, "{landed:?}");
    assert_eq!(twin.stage(7), Some(St::Landing));
}

#[test]
fn out_open_restores_answered_error() {
    for (policy, stage, kind) in [(Policy::Rollback, St::RollingBack(super::outbox_harness::RWhy::Rejected), CommitKind::Rollback), (Policy::Keep, St::MarkingFailed, CommitKind::ErrorMark)] {
        let mut twin = Twin::bare();
        declare(&mut twin);
        let item = StoredOp { answered: Some((41, StoredAnswer::Http { status: 422 })), op: model_op("r", policy, false), ..stored(7, "r") };
        let effects = restore(&mut twin, vec![item]);
        assert_eq!((twin.stage(7), kinds(&effects), sends(&effects)), (Some(stage), vec![kind], 0));
    }
}

#[test]
fn out_open_restores_exhausted() {
    for cause in [None, Some(crate::value::sync::FailureKind::ResponseTooLarge { limit: 64 })] {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let effects = restore(&mut twin, vec![StoredOp { attempts: 40, cause, ..stored(7, "r") }]);
    assert_eq!((twin.stage(7), kinds(&effects), sends(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 0));
    let expected = twin.state().ops[&7].error.as_ref().unwrap();
    if cause.is_some() { assert_eq!(serde_json::from_str::<serde_json::Value>(expected).unwrap()["kind"], "response_too_large:64"); }
    else { assert_eq!(expected, "Exhausted"); }
    assert!(twin.store_text.contains(&format!("{expected:?}")), "{}", twin.store_text);
    }
}

#[test]
fn out_open_restores_pending_offline() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    twin.post(OutboxInput::Online(Online(false)));
    let effects = restore(&mut twin, vec![StoredOp { pending: Some((41, 42)), ..stored(7, "r") }]);
    let readied = ready(&mut twin);
    assert_eq!((twin.stage(7), effects.is_empty(), readied.is_empty()), (Some(St::Queued), true, true));
}

/// P-OUT-14: every restored intent lands in exactly one class, by the order of the table.
#[test]
fn out_prop_restore_classes() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let ops = vec![
        StoredOp { restore_failed: Some("layout".into()), ..stored(1, "a") },
        StoredOp { op: crate::owners::write::outbox::NewOp { action: "act".into(), ..model_op("b", Policy::Rollback, false) }, ..stored(2, "b") },
        StoredOp { answered: Some((41, StoredAnswer::Ok("{}".into()))), ..stored(3, "c") },
        StoredOp { error_mark: true, ..stored(4, "d") },
        StoredOp { attempts: 40, ..stored(5, "e") },
        StoredOp { pending: Some((42, 9)), ..stored(6, "f") },
        stored(7, "g"),
        stored(8, "g"),
    ];
    restore(&mut twin, ops);
    let classes: Vec<_> = (1..=8).map(|id| twin.stage(id)).collect();
    assert_eq!(
        classes,
        vec![Some(St::Failed), Some(St::WaitingDeclaration), Some(St::Landing), Some(St::Failed), Some(St::MarkingFailed), Some(St::PersistingAttempt), Some(St::Queued), Some(St::Queued)]
    );
}

/// P-OUT-13: an answer saved before the close lands without a request, once, on any later input.
#[test]
fn out_prop_restore_answer() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let answered = StoredOp { answered: Some((41, StoredAnswer::Ok("{}".into()))), ..stored(7, "r") };
    let mut effects = restore(&mut twin, vec![answered]);
    effects.extend(ready(&mut twin));
    effects.extend(twin.post(OutboxInput::Online(Online(true))));
    twin.det.tick(crate::runtime::Time(5));
    effects.extend(twin.post(OutboxInput::Response { op_id: 7, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }));
    assert_eq!((sends(&effects), kinds(&effects)), (0, vec![CommitKind::Land]));
}

fn freed_row_twin(attempts: u32) -> Twin {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let dropped = StoredOp { restore_failed: Some("layout".into()), restore_element: Some(("op".into(), "k1".into())), ..stored(7, "r") };
    restore(&mut twin, vec![dropped, StoredOp { attempts, ..stored(8, "r") }]);
    twin
}

fn free_row(twin: &mut Twin) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 42 });
    twin.committed(7)
}

fn attempts_of(effects: &[OutboxEffect], op: u64) -> usize {
    effects.iter().filter(|effect| matches!(effect, OutboxEffect::Store { op_id: Some(found), kind: CommitKind::Attempt, .. } if *found == op)).count()
}

#[test]
fn out_prop_restore_ready_row_freed_once() {
    for attempts in [0, 1] {
        for freed_first in [true, false] {
            let mut twin = freed_row_twin(attempts);
            let (first, second) = if freed_first { (free_row(&mut twin), ready(&mut twin)) } else { let started = ready(&mut twin); (started, free_row(&mut twin)) };
            if freed_first { assert_eq!(sends(&first), 0); }
            if attempts == 0 && freed_first { assert_eq!(attempts_of(&first, 8), 0, "no attempt before RestoreReady: {first:?}"); }
            let all = [first, second].concat();
            if attempts == 0 {
                assert_eq!((attempts_of(&all, 8), sends(&all), twin.stage(8)), (1, 0, Some(St::PersistingAttempt)), "{all:?}");
                assert_eq!(sends(&twin.committed(8)), 1);
            } else {
                assert_eq!((attempts_of(&all, 8), sends(&all), twin.stage(8), twin.state().ops[&8].attempts), (0, 1, Some(St::Sending), attempts), "{all:?}");
            }
        }
    }
}

#[test]
fn out_restore_attempt_number() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let restored = restore(&mut twin, vec![StoredOp { attempts: 3, pending: None, ..stored(7, "r") }]);
    assert_eq!((twin.stage(7), twin.state().ops[&7].attempts, kinds(&restored), sends(&restored)), (Some(St::Queued), 3, vec![], 0));
    let started = ready(&mut twin);
    assert_eq!((twin.stage(7), twin.state().ops[&7].attempts, kinds(&started), sends(&started)), (Some(St::Sending), 3, vec![], 1));
}

#[test]
fn out_restore_ready_once() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    restore(&mut twin, vec![stored(7, "a"), stored(8, "b")]);
    let first = ready(&mut twin);
    let order: Vec<_> = first.iter().filter_map(|effect| match effect { OutboxEffect::Send { op_id, .. } => Some(*op_id), _ => None }).collect();
    assert_eq!((twin.state().phase, kinds(&first), order, twin.stage(7), twin.stage(8)), (Phase::Ready, vec![], vec![7], Some(St::Sending), Some(St::Queued)));
    let again = ready(&mut twin);
    assert_eq!((again.is_empty(), twin.stage(7), twin.stage(8)), (true, Some(St::Sending), Some(St::Queued)));
}

#[test]
fn out_open_restores_pending() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let restored = restore(&mut twin, vec![StoredOp { pending: Some((41, 42)), ..stored(7, "r") }]);
    assert_eq!((twin.stage(7), kinds(&restored), sends(&restored)), (Some(St::PersistingAttempt), Vec::new(), 0), "the held pair classifies to PersistingAttempt with no network before RestoreReady");
    let readied = ready(&mut twin);
    assert_eq!((kinds(&readied), sends(&readied)), (vec![CommitKind::Attempt], 0), "RestoreReady writes the attempt, the request waits for its Committed");
    let started = twin.committed(7);
    let pairs: Vec<_> = started.iter().filter_map(|effect| match effect { OutboxEffect::Send { request_id, sequence, .. } => Some((*request_id, *sequence)), _ => None }).collect();
    assert_eq!((twin.stage(7), pairs), (Some(St::Sending), vec![(41, 42)]), "the held pair repeats with no new sequence");
}

#[test]
fn out_restore_failed_precedes_removed_row() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let item = StoredOp { restore_failed: Some("layout".into()), removed_rows: vec![("Moment".into(), "r".into(), 20)], ..stored(7, "r") };
    let effects = restore(&mut twin, vec![item]);
    assert_eq!((twin.stage(7), kinds(&effects), sends(&effects)), (Some(St::Failed), Vec::new(), 0));
    let effects = twin.post(OutboxInput::Outcome { receipt: Receipt { epoch: EPOCH, op_id: 7, cycle_id: 1, revision: 0, optimistic: None }, reply: 50, discard_on_failure: false });
    assert!(matches!(answers(&effects, 50).as_slice(), [OutboxAnswer::Outcome(OutboxOutcome::Failed { error })] if error == "RestoreFailed"), "{effects:?}");
}

#[test]
fn out_restore_removed_row_removes_saved_answer() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let item = StoredOp { answered: Some((41, StoredAnswer::Ok("{}".into()))), removed_rows: vec![("Moment".into(), "r".into(), 20)], ..stored(7, "r") };
    let effects = restore(&mut twin, vec![item]);
    assert_eq!((twin.stores.len(), sends(&effects)), (1, 0), "{effects:?}");
    assert!(twin.store_text.contains("RemoveIntent { op_id: 7 }") && twin.store_text.contains("RemoveAnswer { request_id: 41 }"), "{}", twin.store_text);
    let completed = twin.committed(7);
    assert_eq!((twin.stage(7), sends(&completed)), (Some(St::Done), 0));
}
