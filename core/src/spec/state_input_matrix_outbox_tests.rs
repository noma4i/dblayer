//! Matrix S for the owner Outbox against `ref_outbox` (whole-owner reference written from the table only): scripts of one operation are played on the reference (`ref_outbox::step` on a full State) and on the owner; after every step the stage of the operation on both sides must be the same, and the second input of each cell is tried after every script. A mismatch is a red cell, not a change of the reference. The adapter between the inputs is the code of this file.
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::outbox_harness::Twin;
use super::ref_outbox::{step, Batch, Input as RIn, Kind, LandFacts, OpSpec, OpOrigin, Phase, Resp, SendKind, State, SubmitFlags};
use super::outbox_harness::Policy;
use crate::owners::write::outbox::OutboxInput;
use crate::test_matrix::Matrix;

#[derive(Clone, Copy, Debug)]
enum Step { Submit, Commit, Response, TemporaryError, PermanentError, RetryDue, Online(bool), EpochEnd, Close }

fn spec() -> OpSpec {
    OpSpec { op_id: None, model: "Moment".into(), row_id: "r".into(), action: "update".into(), expires_after: None, created_at: 0, keep: false, effects_empty: false, destroy: false, response_ignore: false, batch: false, origin: OpOrigin::Api, attempts: 0 }
}

struct Reference { state: State }

impl Reference {
    fn new() -> Self {
        let mut reference = Self { state: State::new() };
        reference.feed(RIn::EpochStarted { epoch: 1, identity: true, generations: Default::default() });
        reference.feed(RIn::RegisterOperations { declarations: vec![("Moment".into(), "update".into()), ("Moment".into(), "create".into()), ("Moment".into(), "destroy".into())] });
        reference.feed(RIn::Restored { epoch: 1, next_seq: 1, queue: Ok(Vec::new()), pending: Vec::new(), pending_lost: false });
        reference.feed(RIn::RestoreReady { epoch: 1 });
        reference.feed(RIn::Online(true));
        assert!(matches!(reference.state.phase, Phase::Ready | Phase::Classified), "the reference is ready");
        reference
    }

    fn feed(&mut self, input: RIn) { self.state = step(&self.state, &input).0; }

    fn stage(&self) -> String { self.state.ops.get(&1).map_or("Done".to_string(), |op| format!("{:?}", op.stage)) }

    fn apply(&mut self, what: Step) {
        let cycle = self.state.ops.get(&1).map_or(1, |op| op.cycle_id);
        let kind = self.state.ops.get(&1).map(|op| match op.stage {
            super::ref_outbox::Stage::Submitting => Kind::Submit,
            super::ref_outbox::Stage::PersistingAttempt => Kind::Attempt,
            super::ref_outbox::Stage::Landing => Kind::Land,
            super::ref_outbox::Stage::RollingBack => Kind::Rollback,
            super::ref_outbox::Stage::MarkingFailed => Kind::MarkFailed,
            super::ref_outbox::Stage::MarkingRetry => Kind::Retry,
            _ => Kind::Submit,
        });
        match what {
            Step::Submit => self.feed(RIn::Submit { epoch: 1, op: spec(), flags: SubmitFlags { valid: true, send: SendKind::Declared, removes_receiver: false, leaves_scope: false, join_missing_row: false, intent_written: false } }),
            Step::Commit => self.feed(RIn::Committed { epoch: 1, batch: Batch::Op(1), kind: kind.unwrap_or(Kind::Submit), revision: 11 }),
            Step::Response => self.feed(RIn::Response { op_id: 1, cycle_id: cycle, resp: Resp::Data(LandFacts::default()), swap: None, pending_retained: None }),
            Step::TemporaryError => self.feed(RIn::TransportError { op_id: 1, cycle_id: cycle, temporary: true, pending_retained: None }),
            Step::PermanentError => self.feed(RIn::TransportError { op_id: 1, cycle_id: cycle, temporary: false, pending_retained: None }),
            Step::RetryDue => self.feed(RIn::Tick { op_id: 1, cycle_id: cycle }),
            Step::Online(on) => self.feed(RIn::Online(on)),
            Step::EpochEnd => self.feed(RIn::EpochEnded(1)),
            Step::Close => self.feed(RIn::Closing),
        }
    }
}

struct Owner { twin: Twin }

impl Owner {
    fn new() -> Self { Self { twin: Twin::new() } }

    fn stage(&self) -> String {
        match self.twin.stage(1) {
            Some(stage) => format!("{stage:?}").split('(').next().unwrap_or("").to_string(),
            None => "Done".into(),
        }
    }

    fn apply(&mut self, what: Step) {
        use crate::owners::write::outbox::OutboxInput as I;
        match what {
            Step::Submit => { self.twin.submit("r", Policy::Rollback, false); }
            Step::Commit => { if self.twin.batches.contains_key(&1) { self.twin.committed(1); } }
            Step::Response => { self.twin.post(I::Response { op_id: 1, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }); }
            Step::TemporaryError => { self.twin.post(I::TransportError { op_id: 1, temporary: true }); }
            Step::PermanentError => { self.twin.post(I::TransportError { op_id: 1, temporary: false }); }
            Step::RetryDue => { self.twin.post(I::RetryDue { op_id: 1, cycle_id: 1 }); }
            Step::Online(on) => { self.twin.post(I::Online(crate::owners::contract::Online(on))); }
            Step::EpochEnd => { self.twin.det.end_epoch(super::outbox_harness::EPOCH, crate::runtime::Cause::SessionReset); self.twin.run(); }
            Step::Close => { self.twin.det.close(); self.twin.run(); }
        }
    }
}

fn scripts() -> Vec<(&'static str, Vec<Step>)> {
    use Step::*;
    vec![
        ("empty", vec![]),
        ("submitted", vec![Submit]),
        ("sending", vec![Submit, Commit]),
        ("landing", vec![Submit, Commit, Response]),
        ("waiting retry", vec![Submit, Commit, TemporaryError]),
        ("persisting attempt", vec![Submit, Commit, TemporaryError, RetryDue]),
        ("marking failed", vec![Submit, Commit, PermanentError]),
        ("offline queued", vec![Online(false), Submit, Commit]),
    ]
}

fn seconds() -> Vec<(&'static str, Option<Step>)> {
    use Step::*;
    vec![("-", None), ("Commit", Some(Commit)), ("Response", Some(Response)), ("TransportError temporary", Some(TemporaryError)), ("TransportError permanent", Some(PermanentError)), ("RetryDue", Some(RetryDue)), ("Offline", Some(Online(false))), ("Online", Some(Online(true))), ("EpochEnded", Some(EpochEnd)), ("Closing", Some(Close))]
}

#[test]
fn matrix_owner_state_input_outbox_v2() {
    let _ = OutboxInput::Online;
    let mut matrix = Matrix::new("S-outbox-v2");
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for (name, script) in scripts() {
        for (second_name, second) in seconds() {
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let (mut reference, mut owner) = (Reference::new(), Owner::new());
                let mut seen = Vec::new();
                for what in script.iter().copied().chain(second) {
                    reference.apply(what);
                    owner.apply(what);
                    seen.push((format!("{what:?}"), reference.stage(), owner.stage()));
                }
                seen
            }));
            let (expected, observed) = match outcome {
                Ok(seen) => match seen.iter().find(|(_, left, right)| left != right) {
                    Some((what, left, right)) => (format!("{what} -> {left}"), format!("{what} -> {right}")),
                    None => ("stages agree".to_string(), "stages agree".to_string()),
                },
                Err(payload) => ("the script runs".to_string(), format!("panic: {}", payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string())).unwrap_or_default().lines().next().unwrap_or("").chars().take(110).collect::<String>())),
            };
            matrix.check(name, "ref_outbox v2", second_name, &expected, &observed);
        }
    }
    std::panic::set_hook(hook);
    matrix.finish();
}
