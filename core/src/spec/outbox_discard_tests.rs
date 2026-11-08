//! Owner-level tests of the `outbox` machine: the discard of an operation that ended `Failed{RestoreFailed}` and the answers no operation owns. Names are the `Test` column.

use super::outbox_harness::*;
use super::ref_outbox::{Policy, St};
use crate::owners::write::outbox::{Fault, OutboxAnswer, OutboxInput, OutboxOutcome, StoredOp};

const DISCARD: u64 = 60;
const API: u64 = 61;

fn dropped() -> Twin {
    let mut twin = Twin::bare();
    twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
    let stored = StoredOp { answered: None, op_id: 7, op: model_op("r", Policy::Rollback, false), error_mark: false, pending: None, attempts: 1, cycle_id: 1, restore_failed: Some("layout".into()), restore_element: Some(("op".into(), "k1".into())) };
    twin.post(OutboxInput::Restored { ops: vec![stored], pending_lost: false });
    twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
    twin
}

fn answers(effects: &[OutboxEffect], reply: u64) -> Vec<OutboxAnswer> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Reply { reply: found, answer } if *found == reply => Some(answer.clone()), _ => None }).collect()
}

#[test]
fn out_failed_discard_restore_failed() {
    let mut twin = dropped();
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: DISCARD });
    assert_eq!(twin.stage(7), Some(St::RollingBack(super::ref_outbox::Why::Discarded)));
    assert!(answers(&effects, DISCARD).is_empty(), "nothing is answered before the commit");
    assert!(twin.store_text.contains("DiscardRestoreFailed { table: \"op\", key: \"k1\" }") && twin.store_text.contains("RemoveIntent"), "{}", twin.store_text);
    assert!(!twin.store_text.contains("RollBack"), "the dropped intent has no optimistic effects to take back: {}", twin.store_text);
}

#[test]
fn out_discard_restore_failed_committed() {
    let mut twin = dropped();
    twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: DISCARD });
    let effects = twin.committed(7);
    assert_eq!(twin.stage(7), Some(St::Done));
    assert_eq!(answers(&effects, DISCARD), vec![OutboxAnswer::Ok]);
    let outcome = twin.post(OutboxInput::Outcome { receipt: twin.receipt(7, EPOCH), reply: WAITER });
    assert_eq!(answers(&outcome, WAITER), vec![OutboxAnswer::Outcome(OutboxOutcome::Discarded)]);
    let again = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: API });
    assert_eq!(answers(&again, API), vec![OutboxAnswer::Err(Fault::NotFound)], "the operation and its element are gone");
}

#[test]
fn out_open_orphan_answer() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::OrphanAnswers { request_ids: vec![41, 42] });
    assert!(twin.store_text.contains("RemoveAnswer { request_id: 41 }") && twin.store_text.contains("RemoveAnswer { request_id: 42 }"), "{}", twin.store_text);
    assert!(twin.state().ops.is_empty(), "the owner holds no operation for them");
    twin.post(OutboxInput::OrphanAnswers { request_ids: Vec::new() });
    assert_eq!(twin.store_text, "", "no answers, no batch");
}
