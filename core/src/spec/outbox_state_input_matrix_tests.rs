//! Matrix S for the owner Outbox: every state of the reference x the base conditions and each condition flipped once x every input. The oracle is the table of `ref_outbox` through `row_check` (next state, replies, effects). A state the harness cannot reach and a state x input pair without a cell of the table are red cells.
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::outbox_harness::row_check;
use super::ref_outbox::{Ctx, In, Policy, Reason, St, Why};
use crate::test_matrix::Matrix;

fn states() -> Vec<St> {
    vec![St::None, St::WaitingDeclaration, St::Submitting, St::Queued, St::PersistingAttempt, St::Sending, St::Landing, St::RollingBack(Why::Rejected), St::RollingBack(Why::Discarded), St::MarkingFailed, St::MarkingRetry, St::Failed, St::Done]
}

fn inputs() -> Vec<In> {
    vec![
        In::Submit, In::Committed, In::Rejected(Reason::StaleGeneration), In::Rejected(Reason::NoSession), In::Rejected(Reason::Storage), In::Rejected(Reason::Closed),
        In::RowFreed, In::Online(true), In::Online(false), In::Discard, In::Retry, In::Outcome, In::ModelsReset, In::Response,
        In::TransportError { temporary: true }, In::TransportError { temporary: false }, In::Dropped, In::Tick, In::EpochEnded, In::Closing,
    ]
}

fn conditions() -> Vec<(&'static str, Ctx)> {
    let base = Ctx::ready();
    vec![
        ("ready", base),
        ("epoch not current", Ctx { epoch_current: false, ..base }),
        ("offline", Ctx { online: false, ..base }),
        ("row busy", Ctx { row_free: false, ..base }),
        ("no slot", Ctx { slot_free: false, ..base }),
        ("expired", Ctx { expired: true, ..base }),
        ("policy keep", Ctx { policy: Policy::Keep, ..base }),
        ("model not owned", Ctx { owns_model: false, ..base }),
        ("not ours", Ctx { ours: false, ..base }),
        ("no attempts left", Ctx { attempts_left: false, ..base }),
        ("transfer", Ctx { transfer: true, ..base }),
    ]
}

fn first_line(payload: &Box<dyn std::any::Any + Send>) -> String {
    let text = payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string())).unwrap_or_default();
    text.lines().next().unwrap_or("").chars().take(140).collect()
}

#[test]
fn matrix_owner_state_input_outbox() {
    let mut matrix = Matrix::new("S-outbox");
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for state in states() {
        for (name, ctx) in conditions() {
            for input in inputs() {
                let outcome = catch_unwind(AssertUnwindSafe(|| row_check(state, ctx, input)));
                let observed = match &outcome { Ok(()) => "owner and table agree".to_string(), Err(payload) => first_line(payload) };
                matrix.check(&format!("{state:?}"), name, &format!("{input:?}"), "owner and table agree", &observed);
            }
        }
    }
    std::panic::set_hook(hook);
    matrix.finish();
}
