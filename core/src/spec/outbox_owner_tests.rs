//! Owner-level tests of the `outbox` machine: the rows of the table of an operation, each against the reference (`ref_outbox.rs`). Names are the `Test` column.



use super::ref_outbox::{step_of, Ctx, In, Obs, Policy, Reason as RReason, St};




use crate::owners::write::outbox::{OutboxAnswer, OutboxInput};


use super::outbox_harness::*;

#[test]
fn out_submit() { row_check(St::None, ready(), In::Submit); }

#[test]
fn out_submit_stale() { row_check(St::None, stale(), In::Submit); }

#[test]
fn out_unknown_operation() { row_check(St::None, ready(), In::Retry); row_check(St::None, ready(), In::Discard); row_check(St::None, ready(), In::Outcome); }

#[test]
fn out_submitting_committed() { row_check(St::Submitting, ready(), In::Committed); }

#[test]
fn out_submitting_committed_queued() { row_check(St::Submitting, offline(), In::Committed); }

#[test]
fn out_submitting_stale_generation() { row_check(St::Submitting, ready(), In::Rejected(RReason::StaleGeneration)); }

#[test]
fn out_submitting_discard() { row_check(St::Submitting, ready(), In::Discard); }

#[test]
fn out_submitting_other() { row_check(St::Submitting, ready(), In::Online(false)); row_check(St::Submitting, ready(), In::ModelsReset); row_check(St::Submitting, ready(), In::Response); row_check(St::Submitting, ready(), In::TransportError { temporary: true }); row_check(St::Submitting, ready(), In::Dropped); row_check(St::Submitting, ready(), In::Tick); }

#[test]
fn out_queued_send() { row_check(St::Queued, offline(), In::Online(true)); }

#[test]
fn out_queued_wait() { row_check(St::Queued, busy(), In::Online(true)); }

#[test]
fn out_queued_offline() { row_check(St::Queued, offline(), In::Online(false)); }

#[test]
fn out_queued_discard() { row_check(St::Queued, offline(), In::Discard); }

#[test]
fn out_queued_retry() { row_check(St::Queued, offline(), In::Retry); }

#[test]
fn out_queued_models_reset() { row_check(St::Queued, offline(), In::ModelsReset); }

#[test]
fn out_queued_models_other() { row_check(St::Queued, offline(), In::ModelsReset); }

#[test]
fn out_queued_stray() { row_check(St::Queued, offline(), In::Response); row_check(St::Queued, offline(), In::TransportError { temporary: true }); row_check(St::Queued, offline(), In::Dropped); row_check(St::Queued, offline(), In::Tick); row_check(St::Queued, offline(), In::Rejected(RReason::Storage)); }

#[test]
fn out_sending_response() { row_check(St::Sending, ready(), In::Response); }

#[test]
fn out_sending_stray() { row_check(St::Sending, stranger(), In::Response); row_check(St::Sending, stranger(), In::TransportError { temporary: true }); row_check(St::Sending, stranger(), In::Dropped); }

#[test]
fn out_sending_temp() { row_check(St::Sending, ready(), In::TransportError { temporary: true }); row_check(St::Sending, ready(), In::Dropped); }

#[test]
fn out_sending_temp_offline() { row_check(St::Sending, offline(), In::TransportError { temporary: true }); row_check(St::Sending, offline(), In::Dropped); }

#[test]
fn out_sending_exhausted_keep() { row_check(St::Sending, Ctx { policy: Policy::Keep, attempts_left: false, ..Ctx::ready() }, In::TransportError { temporary: true }); row_check(St::Sending, Ctx { policy: Policy::Keep, attempts_left: false, ..Ctx::ready() }, In::Dropped); }

#[test]
fn out_sending_reject_rollback() { row_check(St::Sending, ready(), In::TransportError { temporary: false }); }

#[test]
fn out_sending_reject_keep() { row_check(St::Sending, keep(), In::TransportError { temporary: false }); }

#[test]
fn out_sending_tick() { row_check(St::Sending, ready(), In::Tick); }

#[test]
fn out_sending_tick_offline() { row_check(St::Sending, offline(), In::Tick); }

#[test]
fn out_sending_offline() { row_check(St::Sending, ready(), In::Online(false)); }

#[test]
fn out_sending_online() { row_check(St::Sending, ready(), In::Online(true)); }

#[test]
fn out_sending_discard() { row_check(St::Sending, ready(), In::Discard); }

#[test]
fn out_sending_retry() { row_check(St::Sending, ready(), In::Retry); }

#[test]
fn out_sending_other() { row_check(St::Sending, ready(), In::ModelsReset); row_check(St::Sending, ready(), In::Committed); row_check(St::Sending, ready(), In::Rejected(RReason::Storage)); }

#[test]
fn out_landing_committed() { row_check(St::Landing, ready(), In::Committed); }

#[test]
fn out_landing_committed_transfer() { row_check(St::Landing, transfer(), In::Committed); }

#[test]
fn out_landing_stale_generation() { row_check(St::Landing, ready(), In::Rejected(RReason::StaleGeneration)); }

#[test]
fn out_landing_no_session() { row_check(St::Landing, ready(), In::Rejected(RReason::NoSession)); row_check(St::Landing, ready(), In::Rejected(RReason::Closed)); }

#[test]
fn out_landing_storage() { row_check(St::Landing, ready(), In::Rejected(RReason::Storage)); }

#[test]
fn out_landing_tick() { row_check(St::Landing, ready(), In::Tick); }

#[test]
fn out_landing_discard() { row_check(St::Landing, ready(), In::Discard); }

#[test]
fn out_landing_retry() { row_check(St::Landing, ready(), In::Retry); }

#[test]
fn out_landing_other() { row_check(St::Landing, ready(), In::Online(false)); row_check(St::Landing, ready(), In::ModelsReset); row_check(St::Landing, ready(), In::Response); row_check(St::Landing, ready(), In::TransportError { temporary: true }); row_check(St::Landing, ready(), In::Dropped); }

#[test]
fn out_rollback_committed_discard() { row_check(discarding(), transfer(), In::Committed); }

#[test]
fn out_rollback_committed() { row_check(rolling(), transfer(), In::Committed); }

#[test]
fn out_rollback_stale_generation() { row_check(rolling(), ready(), In::Rejected(RReason::StaleGeneration)); }

#[test]
fn out_rollback_no_session() { row_check(rolling(), ready(), In::Rejected(RReason::NoSession)); row_check(rolling(), ready(), In::Rejected(RReason::Closed)); }

#[test]
fn out_rollback_storage() { row_check(rolling(), ready(), In::Rejected(RReason::Storage)); }

#[test]
fn out_rollback_tick() { row_check(rolling(), ready(), In::Tick); }

#[test]
fn out_rollback_discard() { row_check(rolling(), ready(), In::Discard); }

#[test]
fn out_rollback_retry() { row_check(rolling(), ready(), In::Retry); }

#[test]
fn out_rollback_other() { row_check(rolling(), ready(), In::Online(false)); row_check(rolling(), ready(), In::ModelsReset); row_check(rolling(), ready(), In::Response); row_check(rolling(), ready(), In::TransportError { temporary: true }); row_check(rolling(), ready(), In::Dropped); }

#[test]
fn out_failed_retry() { row_check(St::Failed, keep(), In::Retry); }

#[test]
fn out_failed_retry_queued() { row_check(St::Failed, Ctx { online: false, ..keep() }, In::Retry); }

#[test]
fn out_failed_discard() { row_check(St::Failed, keep(), In::Discard); }

#[test]
fn out_failed_mark_result() { row_check(St::Failed, keep(), In::Committed); row_check(St::Failed, keep(), In::Rejected(RReason::Storage)); }

#[test]
fn out_failed_other() { row_check(St::Failed, keep(), In::Online(false)); row_check(St::Failed, keep(), In::ModelsReset); row_check(St::Failed, keep(), In::Response); row_check(St::Failed, keep(), In::TransportError { temporary: true }); row_check(St::Failed, keep(), In::Dropped); row_check(St::Failed, keep(), In::Tick); }

#[test]
fn out_epoch_ended() { row_check(St::Sending, transfer(), In::EpochEnded); }

#[test]
fn out_closing() { row_check(St::Sending, ready(), In::Closing); }

#[test]
fn out_closing_queued() { row_check(St::Queued, offline(), In::Closing); }

#[test]
fn out_outcome_stale() { row_check(St::Sending, stale(), In::Outcome); }

#[test]
fn out_outcome_done() { row_check(St::Done, ready(), In::Outcome); }

#[test]
fn out_outcome_wait() { row_check(St::Sending, ready(), In::Outcome); }

#[test]
fn out_done_refuses() { row_check(St::Done, ready(), In::Discard); row_check(St::Done, ready(), In::Retry); }

#[test]
fn out_done_closing() { row_check(St::Done, ready(), In::Closing); }

#[test]
fn out_done() { row_check(St::Done, ready(), In::Online(true)); row_check(St::Done, ready(), In::ModelsReset); row_check(St::Done, ready(), In::Response); row_check(St::Done, ready(), In::TransportError { temporary: true }); row_check(St::Done, ready(), In::Dropped); row_check(St::Done, ready(), In::Tick); }


#[test]
fn out_submitting_rejected() {
    for reason in [RReason::NoSession, RReason::Closed, RReason::Storage] {
        row_check(St::Submitting, ready(), In::Rejected(reason));
    }
}

#[test]
fn out_submitting_retry() {
    let want = step_of(St::Submitting, &ready(), In::Retry).unwrap();
    let (mut twin, id) = reach(St::Submitting, &ready());
    let before = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 802 });
    let after = twin.committed(id);
    assert_eq!((want.next, want.obs.clone()), (St::Submitting, vec![Obs::Receipt]));
    assert!(before.iter().all(|effect| !matches!(effect, OutboxEffect::Reply { reply: 802, .. })), "no answer before Committed");
    assert!(after.iter().any(|effect| matches!(effect, OutboxEffect::Reply { reply: 802, answer: OutboxAnswer::Receipt(_) })), "the receipt after Committed");
}
