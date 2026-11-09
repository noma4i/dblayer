//! Matrix S for the owner Transfer: scripts of the table (accept, write, copy, steps, failure) are played on the owner and on `ref_transfer`; after every step of a script and after every second input, the stage of the transfer on both sides must be the same. The reference is `ref_transfer::step` written from the table only; a mismatch is a red cell, not a change of the reference. The adapter between the inputs of the reference and the inputs of the owner is the code of this file; a cell can be red because of it, the message tells which side moved.
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::ref_transfer::{step, Cached, Check, Effect, Failure, Input as RefInput, Item, MutOut, Source as RefSource, St, State, Target, TransferKey, ErrKind};
use super::transfer_harness::Twin;
use crate::owners::write::outbox::OutboxOutcome;
use crate::test_matrix::Matrix;

static LOCATION: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

const KEY: &str = "Moment:r1:upload";

#[test]
fn xfer_accepting_closing() {
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferStage};
    for committed in [true, false] {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        let closing = twin.close();
        assert_eq!((twin.stage(id), closing.all_replies(), closing.drained(), closing.writes()), (TransferStage::Accepting, 0, 0, 0));
        let out = if committed { twin.commit() } else { twin.reject(crate::owners::contract::RejectReason::Storage("disk".into())) };
        assert_eq!((twin.stage(id), out.replies(reply), out.drained(), out.copies()), (TransferStage::Done, vec![TransferAnswer::Err(Fault::Interrupted)], 1, 0));
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
}

fn key() -> TransferKey { TransferKey { model: "Moment".into(), row_id: "r1".into(), action: "upload".into() } }

fn failure() -> Failure { Failure { kind: ErrKind::InternalInvalid, message: "refused".into() } }

impl RefRun {
    fn new() -> Self {
        let mut run = Self { state: State::new(1, 100, 10), pending: Vec::new(), reply: 100 };
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
}

impl OwnerRun {
    fn new() -> Self { Self { twin: Twin::new(), id: None } }

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
        if self.id.is_none() && self.stray(step_kind) { return; }
        let id = self.id.unwrap_or(1);
        match step_kind {
            Step::Accept => { let (id, _, _) = self.twin.accepting(KEY, 1); self.id = Some(id); }
            Step::Commit => { if self.twin.has_pending() { self.twin.commit(); } }
            Step::CopyDone => { self.twin.copy_done(id); }
            Step::CopyFailed => { self.twin.copy_failed(id); }
            Step::PartOk => { self.twin.step_ok(id, 1, None); }
            Step::ErrorTemporary => { self.twin.step_error(id, true); }
            Step::ErrorPermanent => { self.twin.step_error(id, false); }
            Step::Completed => { self.twin.completed(id); }
            Step::CompleteErrorTemporary => { self.twin.complete_error(id, true); }
            Step::Cancel => { self.twin.cancel(KEY); }
            Step::Discard => { self.twin.discard(KEY); }
            Step::Retry => { self.twin.retry(KEY, None); }
            Step::OfflineThenOnline => { self.twin.online(false); self.twin.online(true); }
            Step::Offline => { self.twin.online(false); }
            Step::MutationConfirmed => { self.twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 2, result: None, row_id: None }); }
            Step::EndEpoch => { self.twin.end_epoch(); }
            Step::Close => { self.twin.close(); }
        }
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
                    seen.push((format!("{step_kind:?}"), reference.stage(), owner.stage()));
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
