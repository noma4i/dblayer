//! Owner-level tests of the `outbox` machine: the opening classes that classify a restored intent and defer its network to `RestoreReady`. Names are the `Test` column.

use super::outbox_cells_tests::{kinds, persisting, sends, transfer_outcomes};
use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use crate::owners::contract::Online;
use crate::owners::write::outbox::{CommitKind, NewOp, OutboxInput, OutboxOutcome, StoredOp};

fn stored(op_id: u64, row: &str) -> StoredOp {
    StoredOp { answered: None, op_id, op: model_op(row, Policy::Rollback, false), created_at: 0, error_mark: false, pending: None, attempts: 1, cycle_id: 1, cause: None, restore_failed: None, restore_element: None, removed_rows: Vec::new() }
}

fn declare(twin: &mut Twin) { twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())], complete: false }); }

fn restore(twin: &mut Twin, ops: Vec<StoredOp>) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Restored { ops, pending_lost: false }).into_iter().filter(|effect| !matches!(effect, OutboxEffect::RestoreClassified { .. })).collect()
}

fn ready(twin: &mut Twin) -> Vec<OutboxEffect> { twin.post(OutboxInput::RestoreReady { epoch: EPOCH }) }

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
}

#[test]
fn out_restored_pending_goes_offline_before_session_accepts() {
    use crate::owners::contract::{Published, PublishedState};
    use crate::owners::session::{Credential, OperationClass, PendingCall, SecretState, SessionCalls, SessionEffect, SessionInput, SessionPhase, SessionState};
    use crate::runtime::{Address, DetScheduler, EpochStamp};
    use crate::spec::support::{new_log, take, Recorder};

    let mut outbox = Twin::bare();
    declare(&mut outbox);
    restore(&mut outbox, vec![StoredOp { pending: Some((41, 1)), ..stored(7, "r") }]);
    let ready_effects = ready(&mut outbox);
    let sent = if sends(&ready_effects) == 0 { outbox.committed(7) } else { ready_effects };
    assert_eq!((outbox.stage(7), sends(&sent)), (Some(St::Sending), 1));
    assert!(outbox.post(OutboxInput::WhenQuiet { reply: 901 }).is_empty(), "an online request must finish before quiet");
    let offline = outbox.post(OutboxInput::Online(Online(false)));
    assert!(offline.iter().any(|effect| matches!(effect, OutboxEffect::Reply { reply: 901, answer: crate::owners::write::outbox::OutboxAnswer::Ok })), "offline releases the existing waiter: {offline:?}");

    let mut det = DetScheduler::new(1);
    let log = new_log();
    let address: Address<SessionCalls> = det.add("session", SessionState::new(&[], false), Recorder::new(&log));
    let owner = crate::transport::RequestOwner::Operation { seq: 7 };
    let pending = PendingCall { request_id: 41, sequence: 1, class: OperationClass::Authenticated, operation: String::new(), path: "/moments/r".into(), body: "{}".into(), document: None, credential: None, purpose: 0, author: Some(crate::owners::session::OUTBOX_AUTHOR), owner: Some(owner.clone()), epoch: EPOCH, reply: None };
    for input in [
        SessionInput::Published(Published { epoch: EPOCH, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false }),
        SessionInput::Open { credential: Some(Credential { digest: 4, sequence: 0 }), pending: Some(pending), dropped_pending: false, secret: SecretState::Present(4) },
        SessionInput::Snapshot { epoch: EPOCH, online: false },
        SessionInput::Mutate { author: crate::owners::session::OUTBOX_AUTHOR, owner: Some(owner), req_id: 41, epoch: EPOCH, path: "/moments/r".into(), body: "{}".into(), purpose: 0 },
    ] {
        det.post(address, EpochStamp::Control, input);
        det.run_until_quiet(100).unwrap();
    }
    let session_effects: Vec<SessionEffect> = take(&log);
    assert!(matches!(det.state(address).unwrap().phase(), SessionPhase::Calling { phase: crate::owners::session::AttemptPhase::WaitingNetwork, .. }));
    assert!(!session_effects.iter().any(|effect| matches!(effect, SessionEffect::HttpRequest { .. } | SessionEffect::Answer { .. })));
    let quiet = outbox.post(OutboxInput::WhenQuiet { reply: 900 });
    assert!(quiet.iter().any(|effect| matches!(effect, OutboxEffect::Reply { reply: 900, answer: crate::owners::write::outbox::OutboxAnswer::Ok })), "offline and no request in flight must settle: {quiet:?}");
}

#[test]
fn out_open_restores_transfer() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let restored = restore(&mut twin, vec![StoredOp { op: model_op("r", Policy::Rollback, true), ..stored(7, "r") }]);
    assert_eq!((twin.stage(7), sends(&restored)), (Some(St::Queued), 0), "a restored transfer operation classifies with no network before RestoreReady");
    let started = ready(&mut twin);
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
    assert_eq!((twin.stage(7), sends(&restored)), (Some(St::Queued), 0), "the saved intent classifies after a format change, no network before RestoreReady");
    let readied = ready(&mut twin);
    assert!(!twin.store_text.contains("optimistic"), "the optimistic row of a saved intent is not restored: {}", twin.store_text);
    assert_eq!((kinds(&readied), sends(&readied)), (vec![], 1));
    twin.post(OutboxInput::Response { op_id: 7, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert!(twin.store_text.contains("server"), "the row comes from the server answer: {}", twin.store_text);
}

enum Step {
    In(OutboxInput),
    Drain,
    Commit(u64),
}

/// P-OUT-61: one written attempt gives one `Mutate`, wherever the network goes off among the inputs of an operation.
#[test]
fn out_prop_attempt_counted_once_for_any_input_order() {
    use super::outbox_cells_tests::{marking_retry, queued_behind_blocker, sending};
    enum Start {
        Restored,
        BehindBlocker,
        ReservedOffline,
        StoredFresh,
        Persisting,
        Sending,
        MarkingRetry,
    }
    fn offline() -> Step { Step::In(OutboxInput::Online(Online(false))) }
    fn online() -> Step { Step::In(OutboxInput::Online(Online(true))) }
    fn opening(offline_at: usize) -> Vec<Step> {
        let mut steps = vec![Step::In(OutboxInput::Restored { ops: vec![StoredOp { pending: Some((41, 1)), ..stored(7, "r") }], pending_lost: false }), Step::In(OutboxInput::RestoreReady { epoch: EPOCH }), Step::Drain, online(), Step::Drain];
        steps.insert(offline_at, offline());
        steps
    }
    type Steps = Box<dyn Fn(u64) -> Vec<Step>>;
    let cases: Vec<(&str, Start, Steps, (usize, usize, u32))> = vec![
        ("a: offline before Restored", Start::Restored, Box::new(|_| opening(0)), (1, 1, 2)),
        ("b: offline between Restored and RestoreReady", Start::Restored, Box::new(|_| opening(1)), (1, 1, 2)),
        ("c: offline between the attempt batch and its Committed", Start::Restored, Box::new(|_| opening(2)), (1, 1, 2)),
        ("d: a live repeat, offline between the attempt batch and its Committed", Start::Persisting, Box::new(|_| vec![offline(), Step::Drain, online(), Step::Drain]), (0, 1, 2)),
        ("e: a sent attempt fails offline, the repeat writes its own attempt", Start::Sending, Box::new(|id| vec![offline(), Step::In(OutboxInput::TransportError { op_id: id, temporary: true }), Step::Drain, online(), Step::Drain]), (1, 1, 2)),
        ("f: a new cycle of Retry, offline before its Committed", Start::MarkingRetry, Box::new(|_| vec![offline(), Step::Drain, online(), Step::Drain]), (0, 1, 1)),
        ("g: the attempt batch committed while the row is busy, the row is freed", Start::BehindBlocker, Box::new(|id| vec![Step::In(OutboxInput::Response { op_id: id - 1, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }), Step::Commit(id - 1), Step::Drain]), (0, 1, 1)),
        ("h: a Transfer operation with the written attempt, no network and then the network", Start::ReservedOffline, Box::new(|_| vec![online(), Step::Drain]), (0, 1, 1)),
        ("i: open with a StoredOp of attempts 0 and no pair", Start::StoredFresh, Box::new(|_| vec![Step::In(OutboxInput::Restored { ops: vec![StoredOp { attempts: 0, ..stored(7, "r") }], pending_lost: false }), Step::In(OutboxInput::RestoreReady { epoch: EPOCH }), Step::Drain]), (1, 1, 1)),
    ];
    let mut wrong = Vec::new();
    for (case, start, steps, want) in cases {
        let (mut twin, id, mut open_batch) = match start {
            Start::Restored => {
                let mut twin = Twin::bare();
                declare(&mut twin);
                (twin, 7, false)
            }
            Start::BehindBlocker => {
                let mut twin = Twin::new();
                let id = queued_behind_blocker(&mut twin, model_op("r", Policy::Rollback, false));
                (twin, id, false)
            }
            Start::ReservedOffline => {
                let mut twin = Twin::new();
                twin.post(OutboxInput::Online(Online(false)));
                twin.post(OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op: model_op("r", Policy::Rollback, true), created_at: 0 });
                (twin, 4, false)
            }
            Start::StoredFresh => {
                let mut twin = Twin::bare();
                declare(&mut twin);
                (twin, 7, false)
            }
            Start::Persisting => {
                let mut twin = Twin::new();
                let id = persisting(&mut twin, false);
                (twin, id, true)
            }
            Start::Sending => {
                let mut twin = Twin::new();
                let id = sending(&mut twin, Policy::Rollback, false);
                (twin, id, false)
            }
            Start::MarkingRetry => {
                let mut twin = Twin::new();
                let id = marking_retry(&mut twin);
                (twin, id, true)
            }
        };
        let (mut written, mut sent) = (0, 0);
        let mut count = |effects: &[OutboxEffect], open_batch: &mut bool| {
            written += kinds(effects).iter().filter(|kind| **kind == CommitKind::Attempt).count();
            sent += sends(effects);
            *open_batch |= !kinds(effects).is_empty();
        };
        for step in steps(id) {
            match step {
                Step::In(input) => count(&twin.post(input), &mut open_batch),
                Step::Commit(op) => count(&twin.committed(op), &mut open_batch),
                Step::Drain => {
                    for _ in 0..8 {
                        if !open_batch { break; }
                        open_batch = false;
                        count(&twin.committed(id), &mut open_batch);
                    }
                }
            }
        }
        let got = (written, sent, twin.state().ops[&id].attempts);
        if got != want {
            wrong.push(format!("{case}: (attempt batches, Mutate, attempts) = {got:?}, the spec asks {want:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong cases:\n{}", wrong.len(), wrong.join("\n"));
}
