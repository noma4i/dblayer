//! Test of `OutboxExecutor` (`design.md` F3): the answer of a call reaches the platform. Name `exec_out_reply_settles_call`.
//! Store batches, Session `Mutate`, `MutationOutcome` and the retry timer are owner effects, tested in `spec::outbox_owner_tests`.

use crate::schema::OnFailure;
use std::sync::{Arc, Mutex, PoisonError};

use super::outbox::{Fault, LocalWriteKind, NewOp, Origin, Outbox, OutboxAnswer, OutboxInput, OutboxPeers, OutboxState};
use super::rig::peer;
use super::outbox_executor::OutboxExecutor;
use crate::runtime::{DetScheduler, EpochStamp, Time};

const EPOCH: u64 = 1;

#[test]
fn exec_out_reply_settles_call() {
    let answers = Arc::new(Mutex::new(Vec::new()));
    let sink = answers.clone();
    let mut det = DetScheduler::new(3);
    let (store, _) = peer(&mut det, "store");
    let (session, _) = peer(&mut det, "session");
    let (transfer, _) = peer(&mut det, "transfer");
    let outbox = det.add::<Outbox>("outbox", OutboxState::new(EPOCH, OutboxPeers { store, session, transfer: Some(transfer) }), OutboxExecutor { settle: Arc::new(move |reply, answer| sink.lock().unwrap_or_else(PoisonError::into_inner).push((reply, answer))), drained: Arc::new(|_| {}) });
    det.start_epoch(EPOCH);
    let op = NewOp { model: "Moment".into(), row_id: "r1".into(), action: "update".into(), on_failure: OnFailure::Rollback, origin: Origin::Api, path: "/moments/r1".into(), body: String::new(), effects: Default::default() };
    det.post(outbox, EpochStamp::Epoch(EPOCH), OutboxInput::Submit { epoch: EPOCH + 1, op, reply: 1 });
    det.post(outbox, EpochStamp::Epoch(EPOCH), OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH + 1, write: LocalWriteKind::SetLocal, row: "x".into(), values: "{}".into(), reply: 2, now: Time(0) });
    assert!(det.run_until_quiet(100).is_ok());
    assert_eq!(*answers.lock().unwrap_or_else(PoisonError::into_inner), vec![(1, OutboxAnswer::Err(Fault::NoSession)), (2, OutboxAnswer::Err(Fault::NoSession))]);
}
