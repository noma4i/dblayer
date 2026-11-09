//! Matrix S for the owner Transfer: scripts of the table (accept, write, copy, steps, failure) are played on the owner and on `ref_transfer`; after every step of a script and after every second input, the stage and the reply tokens of the transfer on both sides must be the same. Reply tokens keep the full `Ended(cause)` so a bare fault on the owner reads as a red cell. The reference is `ref_transfer::step` written from the table only; a mismatch is a red cell, not a change of the reference. The adapter between the inputs of the reference and the inputs of the owner is the code of this file; a cell can be red because of it, the message tells which side moved.
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::ref_transfer::{step, Answer as RefAnswer, Cached, Check, Effect, Failure, Input as RefInput, Item, MutOut, Source as RefSource, St, State, Target, TransferKey, ErrKind};
use super::transfer_harness::{Out, Twin};
use crate::owners::write::outbox::OutboxOutcome;
use crate::owners::write::transfer::{TransferAnswer, TransferEffect};
use crate::test_matrix::Matrix;

static LOCATION: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

const KEY: &str = "Moment:r1:upload";

/// The reply token both sides compare, keeping the full `Ended(cause)`.
fn outcome_token(text: &str) -> String {
    if text.starts_with("Ended(") { text.to_string() } else { text.split(['(', ' ', '{']).next().unwrap_or("").to_string() }
}

fn ref_answer(answer: &RefAnswer) -> String {
    match answer {
        RefAnswer::Receipt(_) => "Receipt".into(),
        RefAnswer::Receipts(list) => format!("Receipts({})", list.len()),
        RefAnswer::Ok => "Ok".into(),
        RefAnswer::Bool(flag) => format!("Cancelled({flag})"),
        RefAnswer::Err(fault) => format!("Err({})", outcome_token(&format!("{fault:?}"))),
        RefAnswer::Outcome(outcome) => format!("Outcome({})", outcome_token(&format!("{outcome:?}"))),
    }
}

fn own_answer(answer: &TransferAnswer) -> String {
    match answer {
        TransferAnswer::Receipt(_) => "Receipt".into(),
        TransferAnswer::Receipts(list) => format!("Receipts({})", list.len()),
        TransferAnswer::Ok => "Ok".into(),
        TransferAnswer::Cancelled(flag) => format!("Cancelled({flag})"),
        TransferAnswer::Err(fault) => format!("Err({})", outcome_token(&format!("{fault:?}"))),
        TransferAnswer::Outcome(outcome, _) => format!("Outcome({})", outcome_token(&format!("{outcome:?}"))),
    }
}

fn own_replies(out: &Out) -> Vec<String> {
    out.runs.iter().filter_map(|run| if let TransferEffect::Reply { answer, .. } = run { Some(own_answer(answer)) } else { None }).collect()
}

fn line(stage: &str, replies: &[String]) -> String { format!("{stage} [{}]", replies.join("; ")) }

#[test]
fn xfer_accepting_closing() {
    use crate::owners::write::transfer::TransferStage;
    for committed in [true, false] {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        let closing = twin.close();
        assert_eq!((twin.stage(id), closing.all_replies(), closing.drained(), closing.writes()), (TransferStage::Accepting, 0, 0, 0));
        let out = if committed { twin.commit() } else { twin.reject(crate::owners::contract::RejectReason::Storage("disk".into())) };
        assert_eq!((twin.stage(id), format!("{:?}", out.replies(reply)), out.drained(), out.copies()), (TransferStage::Done, "[Err(Ended(Closed))]".into(), 1, 0));
    }
}

#[test]
fn xfer_outcome_unknown() {
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferInput};
    let mut twin = Twin::new();
    let receipt = crate::owners::write::outbox::Receipt { epoch: 1, op_id: 99, cycle_id: 1, revision: 0, optimistic: None };
    let out = twin.post(TransferInput::Outcome { receipt, reply: 20 });
    assert_eq!((out.replies(20), out.all_replies(), out.writes(), out.runs.len()), (vec![TransferAnswer::Err(Fault::NotFound)], 1, 0, 1));
    assert!(twin.state().transfers.is_empty());
}

#[test]
fn xfer_completing_stray() {
    use crate::owners::write::transfer::{TransferInput, TransferStage, TransferTick};
    use crate::runtime::Time;
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    twin.step_ok(id, 2, None);
    twin.commit();
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Completing);
    let attempt_id = twin.attempt(id) + 1;
    twin.ignores(id, vec![
        TransferInput::Completed { transfer_id: id, attempt_id, signed_id: Some("signed".into()) },
        TransferInput::CompleteError { transfer_id: id, attempt_id, temporary: true, error: "error".into() },
        TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { transfer_id: id, attempt_id } },
    ]);
}

#[test]
fn xfer_accepting_rejected_interrupted() {
    use super::transfer_harness::{item, joint, EPOCH};
    use crate::owners::contract::RejectReason;
    use crate::owners::write::transfer::{TransferInput, TransferStage};
    use crate::runtime::Time;
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        let out = twin.reject(reason.clone());
        let cause = if reason == RejectReason::Closed { "Closed" } else { "SessionEnded" };
        assert_eq!((twin.stage(id), format!("{:?}", out.replies(reply)), out.copies()), (TransferStage::Done, format!("[Err({cause})]"), 0));

        let mut twin = Twin::new();
        let keys = [KEY, "Moment:r2:upload"];
        twin.post(TransferInput::AcceptBatch { epoch: EPOCH, items: keys.iter().map(|key| item(key, 2)).collect(), joint: Box::new(joint()), reply: 50, now: Time(0) });
        let ids: Vec<_> = keys.iter().map(|key| twin.state().open_id(key).unwrap()).collect();
        twin.reserve_batch(&ids);
        let out = twin.reject(reason);
        assert_eq!((format!("{:?}", out.replies(50)), out.all_replies(), out.copies()), (format!("[Err({cause})]"), 1, 0));
        assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Done));
    }
}

#[test]
fn xfer_awaiting_source_expires() {
    use super::transfer_harness::{item, DAY, EPOCH};
    use crate::owners::contract::Online;
    use crate::owners::write::outbox::ReserveTarget;
    use crate::owners::write::transfer::{TransferInput, TransferStage, TransferTick};
    use crate::runtime::Time;
    let mut twin = Twin::offline();
    let mut pending = item(KEY, 1);
    pending.source = None;
    pending.steps = None;
    twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(pending), reply: 30, now: Time(0) });
    let id = twin.state().open_id(KEY).unwrap();
    twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::AwaitingSource);
    for input in [
        TransferInput::Online(Online(true)),
        TransferInput::RestoreReady { epoch: EPOCH },
        TransferInput::Tick { now: Time(60 * DAY), kind: TransferTick::Expire },
    ] {
        let out = twin.post(input);
        assert_eq!(twin.stage(id), TransferStage::AwaitingSource);
        assert!(Twin::quiet(&out), "{out:?}");
    }
}

#[test]
fn xfer_accept_after_closing() {
    use super::transfer_harness::{item, joint, EPOCH};
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferInput};
    use crate::runtime::Time;
    let mut twin = Twin::new();
    twin.close();
    for input in [
        TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 1)), reply: 30, now: Time(0) },
        TransferInput::AcceptBatch { epoch: EPOCH, items: vec![item(KEY, 1), item("Moment:r2:upload", 1)], joint: Box::new(joint()), reply: 30, now: Time(0) },
    ] {
        let out = twin.post(input);
        assert_eq!((out.replies(30), out.all_replies(), out.writes()), (vec![TransferAnswer::Err(Fault::Closed)], 1, 0));
        assert!(out.outbox.is_empty() && twin.state().transfers.is_empty());
    }
}

#[test]
fn xfer_orphan_remove_committed() {
    use super::transfer_harness::EPOCH;
    use crate::owners::write::transfer::TransferInput;
    let mut twin = Twin::bare(1);
    twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
    twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: vec![("u9".into(), KEY.into())], batches: Default::default() });
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()]);
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: true });
    assert_eq!(out.orphans_removed(), vec!["u9".to_string()]);
    let out = twin.commit();
    assert!(Twin::quiet(&out), "{out:?}");
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty());
}

#[test]
fn xfer_orphan_abort_refused_keeps_debt() {
    use super::transfer_harness::EPOCH;
    use crate::owners::write::transfer::TransferInput;
    let mut twin = Twin::bare(1);
    twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
    twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: vec![("u9".into(), KEY.into())], batches: Default::default() });
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()]);
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: false });
    assert!(Twin::quiet(&out), "{out:?}");
    twin.online(false);
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()]);
}

fn failed_transfer_mutation(twin: &mut Twin) -> u64 {
    let id = twin.mutating(KEY);
    twin.mutation_outcome(KEY, OutboxOutcome::Failed { error: "error".into() });
    twin.commit();
    assert_eq!(twin.stage(id), crate::owners::write::transfer::TransferStage::Failed);
    id
}

#[test]
fn xfer_failed_discard_outcome() {
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferInput, TransferStage};
    for accepted in [false, true] {
        let mut twin = Twin::new();
        let id = failed_transfer_mutation(&mut twin);
        let (reply, _) = twin.discard(KEY);
        let result = if accepted { Ok(()) } else { Err(Fault::InFlight) };
        let out = twin.post(TransferInput::DiscardOutcome { key: KEY.into(), result });
        assert_eq!(twin.stage(id), TransferStage::Failed);
        if accepted { assert!(Twin::quiet(&out)); }
        else { assert_eq!((out.replies(reply), out.all_replies(), out.writes()), (vec![TransferAnswer::Err(Fault::InFlight)], 1, 0)); }
    }
}

#[test]
fn xfer_failed_discarded() {
    use crate::owners::write::transfer::{TransferAnswer, TransferStage};
    for cancel in [false, true] {
        let mut twin = Twin::new();
        let id = failed_transfer_mutation(&mut twin);
        let (reply, _) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
        let out = twin.mutation_outcome(KEY, OutboxOutcome::Discarded);
        assert_eq!((twin.stage(id), out.removed(), out.rolled_back(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], Vec::<u64>::new(), 0));
        let out = twin.commit();
        let answer = if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
        assert_eq!((twin.stage(id), out.replies(reply)), (TransferStage::Done, vec![answer]));
    }
}

#[test]
fn xfer_failed_confirmed_before_discard() {
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferStage};
    for cancel in [false, true] {
        let mut twin = Twin::new();
        let id = failed_transfer_mutation(&mut twin);
        let (reply, _) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
        let out = twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None });
        let answer = if cancel { TransferAnswer::Cancelled(false) } else { TransferAnswer::Err(Fault::NotFound) };
        assert_eq!((twin.stage(id), out.replies(reply), out.removed()), (TransferStage::Removing, vec![answer], vec![KEY.to_string()]));
    }
}

#[test]
fn xfer_failed_outcome_failed_waiting() {
    use crate::owners::write::transfer::TransferStage;
    for outcome in [OutboxOutcome::Failed { error: "error".into() }, OutboxOutcome::Rejected { error: "error".into() }] {
        let mut twin = Twin::new();
        let id = failed_transfer_mutation(&mut twin);
        twin.discard(KEY);
        let out = twin.mutation_outcome(KEY, outcome);
        assert_eq!(twin.stage(id), TransferStage::Failed);
        assert!(Twin::quiet(&out), "{out:?}");
    }
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Accept,
    Commit,
    CopyDone,
    CopyFailed,
    PartOk,
    ErrorTemporary,
    ErrorPermanent,
    Completed,
    CompleteErrorTemporary,
    Cancel,
    Discard,
    Retry,
    OfflineThenOnline,
    Offline,
    MutationConfirmed,
    EndEpoch,
    Close,
}

struct RefRun {
    state: State,
    pending: Vec<u64>,
    reply: u64,
    replies: Vec<String>,
}

fn key() -> TransferKey { TransferKey { model: "Moment".into(), row_id: "r1".into(), action: "upload".into() } }

fn failure() -> Failure { Failure { kind: ErrKind::InternalInvalid, message: "refused".into() } }

impl RefRun {
    fn new() -> Self {
        let mut run = Self { state: State::new(1, 100, 10), pending: Vec::new(), reply: 100, replies: Vec::new() };
        run.feed(RefInput::RegisterOperations { declarations: [("Moment".to_string(), "upload".to_string())].into_iter().collect() });
        run.feed(RefInput::EpochStarted { epoch: 1, generation: 0 });
        run.feed(RefInput::Restored { epoch: 1, revision: 0, now: 0, orphans: Vec::new(), uploads: Vec::new(), transfer_outcomes: Default::default() });
        run.feed(RefInput::RestoreReady { epoch: 1 });
        run.feed(RefInput::Online(true));
        run
    }

    fn feed(&mut self, input: RefInput) {
        let (state, effects) = step(&self.state, &input);
        for effect in &effects { if let Effect::Store { batch, .. } = effect { self.pending.push(*batch); } }
        for effect in &effects { if let Effect::Reply(_, answer) = effect { self.replies.push(ref_answer(answer)); } }
        self.state = state;
        if effects.iter().any(|effect| matches!(effect, Effect::SelfSlotFreed)) { self.feed(RefInput::SlotFreed); }
    }

    fn next_reply(&mut self) -> u64 { self.reply += 1; self.reply }

    fn attempt(&self) -> u64 {
        match self.state.transfers.get(&1).map(|transfer| &transfer.st) {
            Some(St::Copying { attempt, .. } | St::PersistingAttempt { attempt, .. } | St::Uploading { attempt, .. } | St::Completing { attempt, .. }) => *attempt,
            _ => 0,
        }
    }

    fn stage(&self) -> String {
        match self.state.transfers.get(&1) {
            Some(transfer) => format!("{:?}", transfer.st).split(|character: char| !character.is_alphanumeric()).next().unwrap_or("").to_string(),
            None => "Done".into(),
        }
    }

    fn apply(&mut self, step_kind: Step) {
        self.replies.clear();
        let reply = self.next_reply();
        let attempt = self.attempt();
        match step_kind {
            Step::Accept => {
                let item = Item { key: key(), source: RefSource::File { path: format!("/src/{KEY}") }, values: Vec::new(), check: Check { source_valid: true, action_declared: true, has_file_field: true, row_exists: true, has_effects: true } };
                self.feed(RefInput::Accept { epoch: 1, now: 0, item, reply });
                self.feed(RefInput::OpReserved { target: Target::Transfer { transfer_id: 1 }, result: Ok(1) });
            }
            Step::Commit => { if !self.pending.is_empty() { let batch = self.pending.remove(0); self.feed(RefInput::Committed { batch, epoch: 1, revision: 1 }); } }
            Step::CopyDone => self.feed(RefInput::CopyDone { transfer_id: 1, attempt_id: attempt, size: 10, sha256: "h".into(), cached: Cached::Miss }),
            Step::CopyFailed => self.feed(RefInput::CopyFailed { transfer_id: 1, attempt_id: attempt, error: "unreadable".into() }),
            Step::PartOk => self.feed(RefInput::StepOk { transfer_id: 1, attempt_id: attempt, part: Some(1), etag: Some("e1".into()), upload_id: None }),
            Step::ErrorTemporary => self.feed(RefInput::StepError { transfer_id: 1, attempt_id: attempt, temporary: true, error: failure(), retained: false }),
            Step::ErrorPermanent => self.feed(RefInput::StepError { transfer_id: 1, attempt_id: attempt, temporary: false, error: failure(), retained: false }),
            Step::Completed => self.feed(RefInput::Completed { transfer_id: 1, attempt_id: attempt, signed_id: "signed".into() }),
            Step::CompleteErrorTemporary => self.feed(RefInput::CompleteError { transfer_id: 1, attempt_id: attempt, temporary: true, error: failure() }),
            Step::Cancel => self.feed(RefInput::Cancel { epoch: 1, key: key(), reply }),
            Step::Discard => self.feed(RefInput::Discard { epoch: 1, key: key(), reply }),
            Step::Retry => self.feed(RefInput::Retry { epoch: 1, key: key(), source: None, reply }),
            Step::OfflineThenOnline => { self.feed(RefInput::Online(false)); self.feed(RefInput::Online(true)); }
            Step::Offline => self.feed(RefInput::Online(false)),
            Step::MutationConfirmed => self.feed(RefInput::MutationOutcome(super::ref_transfer::MutTarget::Transfer { transfer_id: 1, outcome: MutOut::Confirmed { revision: 2 } })),
            Step::EndEpoch => self.feed(RefInput::EpochEnded { epoch: 1 }),
            Step::Close => self.feed(RefInput::Closing),
        }
    }
}

struct OwnerRun {
    twin: Twin,
    id: Option<u64>,
    replies: Vec<String>,
}

impl OwnerRun {
    fn new() -> Self { Self { twin: Twin::new(), id: None, replies: Vec::new() } }

    fn stage(&self) -> String {
        match self.id {
            Some(id) => format!("{:?}", self.twin.stage(id)),
            None => "Done".into(),
        }
    }

    /// The inputs that name a transfer no one accepted: the owner must treat them as strangers.
    fn stray(&mut self, step_kind: Step) -> bool {
        use crate::owners::write::transfer::TransferInput;
        use crate::store::files::Location;
        let (transfer_id, attempt_id) = (999, 1);
        let input = match step_kind {
            Step::CopyDone => TransferInput::CopyDone { transfer_id, attempt_id, size: 10, sha256: "h".into(), place: Location::Directory, cached: crate::owners::write::transfer::CachedBlob::Miss },
            Step::CopyFailed => TransferInput::CopyFailed { transfer_id, attempt_id, error: "unreadable".into() },
            Step::PartOk => TransferInput::StepOk { transfer_id, attempt_id, part: Some(1), upload_id: None, etag: Some("e1".into()) },
            Step::ErrorTemporary | Step::ErrorPermanent => TransferInput::StepError { transfer_id, attempt_id, temporary: matches!(step_kind, Step::ErrorTemporary), error: super::transfer_harness::refusal() },
            Step::Completed => TransferInput::Completed { transfer_id, attempt_id, signed_id: Some("signed".into()) },
            Step::CompleteErrorTemporary => TransferInput::CompleteError { transfer_id, attempt_id, temporary: true, error: super::transfer_harness::refusal() },
            Step::MutationConfirmed => return true,
            _ => return false,
        };
        self.twin.post(input);
        true
    }

    fn apply(&mut self, step_kind: Step) {
        self.replies.clear();
        if self.id.is_none() && self.stray(step_kind) { return; }
        let id = self.id.unwrap_or(1);
        let out: Option<Out> = match step_kind {
            Step::Accept => { let (id, _, out) = self.twin.accepting(KEY, 1); self.id = Some(id); Some(out) }
            Step::Commit => { if self.twin.has_pending() { Some(self.twin.commit()) } else { None } }
            Step::CopyDone => Some(self.twin.copy_done(id)),
            Step::CopyFailed => Some(self.twin.copy_failed(id)),
            Step::PartOk => Some(self.twin.step_ok(id, 1, None)),
            Step::ErrorTemporary => Some(self.twin.step_error(id, true)),
            Step::ErrorPermanent => Some(self.twin.step_error(id, false)),
            Step::Completed => Some(self.twin.completed(id)),
            Step::CompleteErrorTemporary => Some(self.twin.complete_error(id, true)),
            Step::Cancel => Some(self.twin.cancel(KEY).1),
            Step::Discard => Some(self.twin.discard(KEY).1),
            Step::Retry => Some(self.twin.retry(KEY, None).1),
            Step::OfflineThenOnline => { self.twin.online(false); Some(self.twin.online(true)) }
            Step::Offline => Some(self.twin.online(false)),
            Step::MutationConfirmed => Some(self.twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 2, result: None, row_id: None })),
            Step::EndEpoch => Some(self.twin.end_epoch()),
            Step::Close => Some(self.twin.close()),
        };
        if let Some(out) = out { self.replies = own_replies(&out); }
    }
}

fn scripts() -> Vec<(&'static str, Vec<Step>)> {
    use Step::*;
    vec![
        ("empty", vec![]),
        ("accepted", vec![Accept]),
        ("copying", vec![Accept, Commit]),
        ("persisting copy", vec![Accept, Commit, CopyDone]),
        ("uploading", vec![Accept, Commit, CopyDone, Commit]),
        ("persisting failure", vec![Accept, Commit, CopyDone, Commit, ErrorPermanent]),
        ("failed", vec![Accept, Commit, CopyDone, Commit, ErrorPermanent, Commit]),
        ("waiting retry", vec![Accept, Commit, CopyDone, Commit, ErrorTemporary]),
        ("copy failed", vec![Accept, Commit, CopyFailed]),
        ("completing", vec![Accept, Commit, CopyDone, Commit, PartOk, Commit, Commit]),
        ("persisting complete", vec![Accept, Commit, CopyDone, Commit, PartOk, Commit, Commit, Completed]),
        ("mutating", vec![Accept, Commit, CopyDone, Commit, PartOk, Commit, Commit, Completed, Commit]),
    ]
}

fn second_inputs() -> Vec<(&'static str, Option<Step>)> {
    use Step::*;
    vec![("-", None), ("Commit", Some(Commit)), ("CopyDone", Some(CopyDone)), ("StepOk", Some(PartOk)), ("StepError temporary", Some(ErrorTemporary)), ("StepError permanent", Some(ErrorPermanent)), ("Completed", Some(Completed)), ("CompleteError temporary", Some(CompleteErrorTemporary)), ("Cancel", Some(Cancel)), ("Discard", Some(Discard)), ("Retry", Some(Retry)), ("Offline", Some(Offline)), ("Offline then online", Some(OfflineThenOnline)), ("MutationOutcome confirmed", Some(MutationConfirmed)), ("EpochEnded", Some(EndEpoch)), ("Closing", Some(Close))]
}

fn message(payload: &(dyn std::any::Any + Send)) -> String {
    let text = payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string())).unwrap_or_default();
    text.lines().next().unwrap_or("").chars().take(120).collect()
}

#[test]
fn matrix_owner_state_input_transfer() {
    let mut matrix = Matrix::new("S-transfer");
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|info| { *LOCATION.lock().unwrap_or_else(|error| error.into_inner()) = info.location().map(|place| format!("{}:{}", place.file(), place.line())).unwrap_or_default(); }));
    for (name, script) in scripts() {
        for (second_name, second) in second_inputs() {
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let (mut reference, mut owner) = (RefRun::new(), OwnerRun::new());
                let mut seen = Vec::new();
                for step_kind in script.iter().copied().chain(second) {
                    reference.apply(step_kind);
                    owner.apply(step_kind);
                    seen.push((format!("{step_kind:?}"), line(&reference.stage(), &reference.replies), line(&owner.stage(), &owner.replies)));
                }
                seen
            }));
            let (expected, observed) = match outcome {
                Ok(seen) => {
                    let wrong = seen.iter().find(|(_, left, right)| left != right);
                    match wrong { Some((step_kind, left, right)) => (format!("{step_kind} -> {left}"), format!("{step_kind} -> {right}")), None => ("stages agree".to_string(), "stages agree".to_string()) }
                }
                Err(payload) => ("the script runs".to_string(), format!("panic: {} at {}", message(payload.as_ref()), LOCATION.lock().unwrap_or_else(|error| error.into_inner()))),
            };
            matrix.check(name, "ref_transfer", second_name, &expected, &observed);
        }
    }
    std::panic::set_hook(hook);
    matrix.finish();
}
