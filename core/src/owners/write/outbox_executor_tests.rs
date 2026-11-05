//! Test of `OutboxExecutor` (`design.md` F3): the answer of a call reaches the platform. Name `exec_out_reply_settles_call`.
//! Store batches, Session `Mutate`, `MutationOutcome`, `Drained` and the retry timer are owner effects in the next step.

use std::sync::{Arc, Mutex, PoisonError};

use super::outbox::{Fault, LocalWriteKind, NewOp, OnReject, Origin, Outbox, OutboxAnswer, OutboxInput, OutboxState};
use super::outbox_executor::OutboxExecutor;
use crate::runtime::{DetScheduler, EpochStamp, Time};

const EPOCH: u64 = 1;

#[test]
fn exec_out_reply_settles_call() {
    let answers = Arc::new(Mutex::new(Vec::new()));
    let sink = answers.clone();
    let mut det = DetScheduler::new(3);
    let outbox = det.add::<Outbox>("outbox", OutboxState::new(EPOCH), OutboxExecutor { settle: Arc::new(move |reply, answer| sink.lock().unwrap_or_else(PoisonError::into_inner).push((reply, answer))) });
    det.start_epoch(EPOCH);
    let op = NewOp { model: "Moment".into(), row_id: "r1".into(), action: "update".into(), on_reject: OnReject::Rollback, origin: Origin::Api };
    det.post(outbox, EpochStamp::Epoch(EPOCH), OutboxInput::Submit { epoch: EPOCH + 1, op, reply: 1 });
    det.post(outbox, EpochStamp::Epoch(EPOCH), OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH + 1, write: LocalWriteKind::SetLocal, row: "x".into(), reply: 2, now: Time(0) });
    assert!(det.run_until_quiet(100).is_ok());
    assert_eq!(*answers.lock().unwrap_or_else(PoisonError::into_inner), vec![(1, OutboxAnswer::Err(Fault::NoSession)), (2, OutboxAnswer::Err(Fault::NoSession))]);
}
