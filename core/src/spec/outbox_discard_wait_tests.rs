//! Owner-level tests of the `outbox` machine: `Discard` in the stages of the internal steps (D-392).

use super::outbox_cells_tests::{answers, marking_failed, marking_retry, persisting};
use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use crate::owners::contract::Online;
use crate::owners::write::outbox::{Fault, OpId, OutboxAnswer, OutboxInput};

fn discard() -> OutboxInput { OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY } }

/// In each stage of an internal step `Discard` gets no answer; the exit of the stage runs it by the row of the stage it leaves to: the rollback and `Ok` for an unsent operation, `Err(InFlight)` once the request left.
/// Observable: the answers and the sends. Catches: a `Discard` refused by the stage it met, a command lost at the exit, an answer twice.
#[test]
fn out_discard_waits_for_the_stage() {
    for stage in [St::Submitting, St::PersistingAttempt, St::MarkingFailed, St::MarkingRetry] {
        let mut twin = Twin::new();
        let id = match stage {
            St::Submitting => twin.submit("r", Policy::Rollback, false),
            St::PersistingAttempt => persisting(&mut twin, false),
            St::MarkingFailed => marking_failed(&mut twin, false),
            _ => marking_retry(&mut twin),
        };
        let waiting = twin.post(discard());
        assert!(answers(&waiting, DISCARD_REPLY).is_empty() && waiting.is_empty(), "{stage:?}: the command waits");
        let mut effects = Vec::new();
        for _ in 0..4 {
            if matches!(twin.stage(id), Some(St::Submitting | St::PersistingAttempt | St::MarkingFailed | St::MarkingRetry | St::RollingBack(_))) { effects.extend(twin.committed(id)); } else { break; }
        }
        let expected = if sends_of(&effects) > 0 { OutboxAnswer::Err(Fault::InFlight) } else { OutboxAnswer::Ok };
        assert_eq!(answers(&effects, DISCARD_REPLY), vec![expected], "{stage:?}");
    }
}

/// P-OUT-D392: `Discard` at every point of the trace of an operation answers once: `Ok` and the rollback while its request has not left, `Err(InFlight)` after it left.
#[test]
fn out_prop_discard_at_any_point() {
    for point in 0..4 {
        let mut twin = Twin::new();
        twin.post(OutboxInput::Online(Online(false)));
        let id: OpId = twin.submit("r", Policy::Rollback, false);
        let mut effects = Vec::new();
        if point > 0 { effects.extend(twin.committed(id)); }
        if point > 1 { effects.extend(twin.post(OutboxInput::Online(Online(true)))); }
        if point > 2 { effects.extend(twin.committed(id)); }
        effects.extend(twin.post(discard()));
        for _ in 0..4 {
            if matches!(twin.stage(id), Some(St::Submitting | St::PersistingAttempt | St::RollingBack(_))) { effects.extend(twin.committed(id)); } else { break; }
        }
        let expected = if sends_of(&effects) > 0 { OutboxAnswer::Err(Fault::InFlight) } else { OutboxAnswer::Ok };
        assert_eq!(answers(&effects, DISCARD_REPLY), vec![expected], "point {point}");
        assert_eq!(point < 2, sends_of(&effects) == 0, "point {point}: the request leaves with the commit of the attempt");
    }
}
