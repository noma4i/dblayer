//! Matrix S for the opening of the Outbox against `ref_outbox` (v2 `restore.rs`): every kind of restored operation x the order of the declaration of operations x the order of the network. Both sides get the same stored operation, then `RestoreReady`, the network input and a `Commit` of the batch the opening wrote. After every step the stage of the operation, the ordered effects and the ordered replies must be equal. A mismatch is a red cell, the reference is never changed.
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::outbox_harness::{model_op, Policy, Twin, EPOCH};
use super::ref_outbox::{step, Batch, Input as RIn, Kind, RestoreItem, Saved, State};
use super::state_input_matrix_outbox_tests::{first_line, own_tokens, ref_tokens, spec, split_line};
use crate::owners::contract::Online;
use crate::owners::write::outbox::{OutboxInput, StoredOp};
use crate::store::session_state::StoredAnswer;
use crate::test_matrix::Matrix;

#[derive(Clone, Copy, Debug)]
enum Kind2 { Plain, NoColumns, ErrorMark, Dropped, Pending, AnsweredOk, AnsweredRefused, Exhausted, Expired }

fn kinds() -> Vec<(&'static str, Kind2)> {
    vec![("plain", Kind2::Plain), ("layout without attempt columns", Kind2::NoColumns), ("error mark", Kind2::ErrorMark), ("dropped by the layout", Kind2::Dropped), ("pending pair", Kind2::Pending), ("saved answer Ok", Kind2::AnsweredOk), ("saved answer refused", Kind2::AnsweredRefused), ("attempts exhausted", Kind2::Exhausted), ("expired before the first send", Kind2::Expired)]
}

fn item(kind: Kind2) -> RestoreItem {
    let mut op = spec(1, false, false);
    let (mut attempts, mut sent, mut has_error_mark, mut restore_failed, mut answer) = (1, false, false, false, None);
    match kind {
        Kind2::Plain => {}
        Kind2::NoColumns => attempts = 0,
        Kind2::ErrorMark => has_error_mark = true,
        Kind2::Dropped => restore_failed = true,
        Kind2::Pending => sent = true,
        Kind2::AnsweredOk => answer = Some(Saved::Ok),
        Kind2::AnsweredRefused => answer = Some(Saved::Refused),
        Kind2::Exhausted => attempts = 39,
        Kind2::Expired => { attempts = 0; op.expires_after = Some(10_000); }
    }
    RestoreItem { op_id: 1, spec: op, cycle_id: 1, request_id: 41, attempts, sent, restore_failed, has_error_mark, answer }
}

fn stored(kind: Kind2) -> StoredOp {
    let mut op = model_op("r", Policy::Rollback, false);
    let (mut attempts, mut pending, mut error_mark, mut restore_failed, mut answered) = (1, None, false, None, None);
    match kind {
        Kind2::Plain => {}
        Kind2::NoColumns => attempts = 0,
        Kind2::ErrorMark => error_mark = true,
        Kind2::Dropped => restore_failed = Some("layout".to_string()),
        Kind2::Pending => pending = Some((41, 1)),
        Kind2::AnsweredOk => answered = Some((41, StoredAnswer::Ok("{\"data\":{\"row\":\"saved\"}}".into()))),
        Kind2::AnsweredRefused => answered = Some((41, StoredAnswer::Http { status: 422 })),
        Kind2::Exhausted => attempts = 39,
        Kind2::Expired => { attempts = 0; op.expires_after = Some(std::time::Duration::from_secs(10)); }
    }
    StoredOp { answered, op_id: 1, op, error_mark, pending, attempts, cycle_id: 1, cause: None, restore_element: restore_failed.as_ref().map(|_| ("op".to_string(), "k1".to_string())), restore_failed, removed_rows: Vec::new() }
}

#[derive(Clone, Copy, Debug)]
enum Phase { Declare, Restore, Ready, Online(bool), Age, Commit }

fn plan(declare_first: bool, network: &str) -> Vec<Phase> {
    let mut steps = Vec::new();
    if network == "online before the opening" { steps.push(Phase::Online(true)); }
    if network == "offline before the opening" { steps.push(Phase::Online(false)); }
    if declare_first { steps.push(Phase::Declare); }
    steps.push(Phase::Restore);
    if !declare_first { steps.push(Phase::Declare); }
    steps.push(Phase::Ready);
    steps.push(Phase::Age);
    steps.push(Phase::Commit);
    steps
}

fn declarations() -> Vec<(String, String)> { vec![("Moment".into(), "update".into()), ("Moment".into(), "create".into()), ("Moment".into(), "destroy".into())] }

fn ref_run(kind: Kind2, steps: &[Phase]) -> Vec<String> {
    let mut state = State::new();
    state = step(&state, &RIn::EpochStarted { epoch: 1, identity: true, generations: Default::default() }).0;
    let mut lines = Vec::new();
    for what in steps {
        let input = match what {
            Phase::Declare => RIn::RegisterOperations { declarations: declarations() },
            Phase::Restore => RIn::Restored { epoch: 1, next_seq: 2, queue: Ok(vec![item(kind)]), pending: if matches!(kind, Kind2::Pending) { vec![41] } else { Vec::new() }, pending_lost: false },
            Phase::Ready => RIn::RestoreReady { epoch: 1 },
            Phase::Online(on) => RIn::Online(*on),
            Phase::Age => RIn::Now(11_000),
            Phase::Commit => {
                let kind_now = match state.ops.get(&1).map(|op| op.stage) {
                    Some(super::ref_outbox::Stage::PersistingAttempt) => Kind::Attempt,
                    Some(super::ref_outbox::Stage::Landing) => Kind::Land,
                    Some(super::ref_outbox::Stage::RollingBack) => Kind::Rollback,
                    Some(super::ref_outbox::Stage::MarkingFailed) => Kind::MarkFailed,
                    _ => Kind::Submit,
                };
                RIn::Committed { epoch: 1, batch: Batch::Op(1), kind: kind_now, revision: 11 }
            }
        };
        let (next, effects) = step(&state, &input);
        state = next;
        let stages = state.ops.values().map(|op| format!("{}:{:?}", op.id, op.stage)).collect::<Vec<_>>().join(",");
        lines.push(split_line(&stages, &ref_tokens(&effects)));
    }
    lines
}

fn own_run(kind: Kind2, steps: &[Phase]) -> Vec<String> {
    let mut twin = Twin::bare();
    let mut lines = Vec::new();
    for what in steps {
        let effects = match what {
            Phase::Declare => twin.post(OutboxInput::RegisterOperations { declarations: declarations() }),
            Phase::Restore => twin.post(OutboxInput::Restored { ops: vec![stored(kind)], pending_lost: false }),
            Phase::Ready => twin.post(OutboxInput::RestoreReady { epoch: EPOCH }),
            Phase::Online(on) => twin.post(OutboxInput::Online(Online(*on))),
            Phase::Age => twin.post(OutboxInput::Tick(crate::runtime::Time(11_000))),
            Phase::Commit => if twin.batches.contains_key(&1) { twin.committed(1) } else { Vec::new() },
        };
        let mut effects = effects;
        if matches!(kind, Kind2::AnsweredOk) && matches!(what, Phase::Restore | Phase::Declare) && twin.stage(1) == Some(super::outbox_harness::St::Landing) && !effects.iter().any(|effect| matches!(effect, super::outbox_harness::OutboxEffect::Store { .. })) {
            effects.extend(twin.post(OutboxInput::Response { op_id: 1, rows: super::outbox_harness::server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }));
        }
        let mut ids: Vec<u64> = twin.state().ops.keys().copied().collect();
        ids.sort_unstable();
        let stages = ids.iter().map(|id| format!("{id}:{}", twin.stage(*id).map_or("Done".to_string(), |stage| format!("{stage:?}").split('(').next().unwrap_or("").to_string()))).collect::<Vec<_>>().join(",");
        lines.push(split_line(&stages, &own_tokens(&effects)));
    }
    lines
}

#[test]
fn matrix_owner_state_input_restore() {
    let mut matrix = Matrix::new("S-restore");
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for (name, kind) in kinds() {
        for declare_first in [true, false] {
            for network in ["online before the opening", "offline before the opening"] {
                let steps = plan(declare_first, network);
                let outcome = catch_unwind(AssertUnwindSafe(|| (ref_run(kind, &steps), own_run(kind, &steps))));
                let (expected, observed) = match outcome {
                    Ok((left, right)) => match left.iter().zip(right.iter()).enumerate().find(|(_, (a, b))| a != b) {
                        Some((at, (a, b))) => (format!("{:?} -> {a}", steps[at]), format!("{:?} -> {b}", steps[at])),
                        None => ("agree".to_string(), "agree".to_string()),
                    },
                    Err(payload) => ("the script runs".to_string(), format!("panic: {}", first_line(payload.as_ref()))),
                };
                matrix.check(&format!("{name} / {}", if declare_first { "declared first" } else { "declared after" }), "ref_outbox restore", network, &expected, &observed);
            }
        }
    }
    std::panic::set_hook(hook);
    matrix.finish();
}
