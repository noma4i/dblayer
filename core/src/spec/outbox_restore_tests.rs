//! Owner-level tests of the `outbox` machine: the phase `Restoring` with its held calls, the classes of the opening and the declarations. Names are the `Test` column.

use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use crate::owners::contract::{Committed, EpochStarted, ModelsReset, Online, RejectReason, Rejected};
use crate::owners::session::StoredAnswer;
use crate::owners::write::outbox::{CommitKind, Fault, OutboxAnswer, OutboxInput, OutboxOutcome, Phase, Receipt, StoredOp};
use crate::runtime::Cause;

fn stored(op_id: u64, row: &str) -> StoredOp {
    StoredOp { answered: None, op_id, op: model_op(row, Policy::Rollback, false), error_mark: false, pending: None, attempts: 1, cycle_id: 1, restore_failed: None, restore_element: None }
}

fn declare(twin: &mut Twin) { twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] }); }

fn restore(twin: &mut Twin, ops: Vec<StoredOp>) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Restored { ops, pending_lost: false }).into_iter().filter(|effect| !matches!(effect, OutboxEffect::RestoreClassified { .. })).collect()
}

fn ready(twin: &mut Twin) -> Vec<OutboxEffect> { twin.post(OutboxInput::RestoreReady { epoch: EPOCH }) }

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
        OutboxInput::Outcome { receipt: Receipt { epoch: EPOCH, op_id: 99, cycle_id: 1, revision: 0, optimistic: None }, reply: 43 },
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
fn out_restoring_models_reset() {
    let mut twin = Twin::bare();
    assert!(twin.post(OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 7, models: vec!["Moment".into()] })).is_empty());
    assert_eq!(twin.state().generation.get("Moment"), Some(&7));
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
    assert_eq!(answers(&ended, 43), vec![OutboxAnswer::Outcome(OutboxOutcome::Interrupted)]);
    let late = twin.post(OutboxInput::Restored { ops: vec![stored(7, "r")], pending_lost: false });
    assert!(late.is_empty(), "a late Restored of the ended epoch is a foreign input and tells nobody: {late:?}");
}

#[test]
fn out_restoring_foreign() {
    let mut twin = Twin::bare();
    let inputs = vec![
        OutboxInput::RetryDue { op_id: 1, cycle_id: 1 },
        OutboxInput::Committed(Committed { batch: 1, revision: 1, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }),
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
    assert_eq!((twin.state().phase, twin.stage(7), sends(&effects), kinds(&effects).len()), (Phase::Classified, Some(St::PersistingAttempt), 0, 0), "{effects:?}");
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
    assert_eq!(answers(&closed, 43), vec![OutboxAnswer::Outcome(OutboxOutcome::Interrupted)]);
    assert_eq!(drained(&closed), 1);
}

#[test]
fn out_restoring_register() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    restore(&mut twin, vec![stored(7, "r")]);
    assert_eq!(twin.stage(7), Some(St::PersistingAttempt), "the class is read from the declarations that arrived while Restoring");
}

#[test]
fn out_restore_ready() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    restore(&mut twin, vec![stored(7, "a"), stored(8, "b")]);
    let effects = ready(&mut twin);
    assert_eq!((twin.state().phase, kinds(&effects)), (Phase::Ready, vec![CommitKind::Attempt]));
    let order: Vec<_> = effects.iter().filter_map(|effect| match effect { OutboxEffect::Store { op_id, .. } => *op_id, _ => None }).collect();
    assert_eq!((order, twin.stage(8)), (vec![7], Some(St::Queued)), "the lowest seq takes the one slot, the next waits");
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
    assert_eq!((twin.stage(7), kinds(&effects)), (Some(St::PersistingAttempt), vec![CommitKind::Attempt]));
}

#[test]
fn out_register_still_waiting() {
    let mut twin = Twin::bare();
    restore(&mut twin, vec![stored(7, "r")]);
    let effects = twin.post(OutboxInput::RegisterOperations { declarations: vec![("Chat".into(), "send".into())] });
    assert_eq!((twin.stage(7), effects.is_empty()), (Some(St::WaitingDeclaration), true));
}

#[test]
fn out_register_other() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let effects = twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
    assert_eq!((twin.stage(id), effects.is_empty()), (Some(St::Sending), true));
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
fn out_waiting_declaration_other() {
    let mut twin = Twin::bare();
    restore(&mut twin, vec![stored(7, "r")]);
    ready(&mut twin);
    let inputs = vec![
        OutboxInput::Online(Online(true)),
        OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Moment".into()] }),
        OutboxInput::Response { op_id: 7, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: 7, temporary: true },
        OutboxInput::Dropped { op_id: 7 },
        OutboxInput::RetryDue { op_id: 7, cycle_id: 1 },
    ];
    for input in inputs {
        let effects = twin.post(input.clone());
        assert_eq!((twin.stage(7), effects.is_empty()), (Some(St::WaitingDeclaration), true), "{input:?}");
    }
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
fn out_open_restores_dropped() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    restore(&mut twin, vec![StoredOp { restore_failed: Some("layout".into()), ..stored(7, "r") }]);
    ready(&mut twin);
    assert_eq!(twin.stage(7), Some(St::Failed));
    let effects = twin.post(OutboxInput::Outcome { receipt: Receipt { epoch: EPOCH, op_id: 7, cycle_id: 1, revision: 0, optimistic: None }, reply: 50 });
    assert!(matches!(answers(&effects, 50).as_slice(), [OutboxAnswer::Outcome(OutboxOutcome::Failed { error })] if error == "RestoreFailed"), "{effects:?}");
}

#[test]
fn out_open_waiting_declaration() {
    let mut twin = Twin::bare();
    let effects = restore(&mut twin, vec![stored(7, "r")]);
    assert_eq!((twin.stage(7), effects.is_empty()), (Some(St::WaitingDeclaration), true));
}

#[test]
fn out_failed_retry_state_lost() { out_open_restores_state_lost(); }

#[test]
fn out_open_restores_state_lost() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let effects = twin.post(OutboxInput::Restored { ops: vec![stored(7, "r")], pending_lost: true });
    assert_eq!((twin.stage(7), kinds(&effects).len()), (Some(St::Failed), 0), "no batch: the mark is derived at every open");
    ready(&mut twin);
    let retry = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 41 });
    assert_eq!(answers(&retry, 41), vec![OutboxAnswer::Err(Fault::StateLost)]);
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
    let mut twin = Twin::bare();
    declare(&mut twin);
    let effects = restore(&mut twin, vec![StoredOp { attempts: 40, ..stored(7, "r") }]);
    assert_eq!((twin.stage(7), kinds(&effects), sends(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 0));
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
    effects.extend(twin.post(OutboxInput::Tick(crate::runtime::Time(5))));
    effects.extend(twin.post(OutboxInput::Response { op_id: 7, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }));
    assert_eq!((sends(&effects), kinds(&effects)), (0, vec![CommitKind::Land]));
}

fn freed_row_twin() -> Twin {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let dropped = StoredOp { restore_failed: Some("layout".into()), restore_element: Some(("op".into(), "k1".into())), ..stored(7, "r") };
    restore(&mut twin, vec![dropped, stored(8, "r")]);
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
    for freed_first in [true, false] {
        let mut twin = freed_row_twin();
        let mut all = Vec::new();
        let (first, second) = if freed_first { (free_row(&mut twin), ready(&mut twin)) } else { let started = ready(&mut twin); (started, free_row(&mut twin)) };
        let before_ready = if freed_first { &first } else { &second };
        assert!(before_ready.iter().all(|effect| !matches!(effect, OutboxEffect::Send { .. })) && (!freed_first || attempts_of(&first, 8) == 0), "freed_first={freed_first}: no write of the attempt before RestoreReady: {first:?}");
        all.extend(first);
        all.extend(second);
        assert_eq!((attempts_of(&all, 8), sends(&all), twin.stage(8)), (1, 0, Some(St::PersistingAttempt)), "freed_first={freed_first}: {all:?}");
        let started = twin.committed(8);
        assert_eq!((sends(&started), twin.stage(8)), (1, Some(St::Sending)), "freed_first={freed_first}: {started:?}");
    }
}
