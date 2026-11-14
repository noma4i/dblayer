use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use crate::owners::contract::{ErrorClass, Online, RequestId};
use crate::owners::session::{AuthorOutcome, SessionError, SessionInput};
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{OpId, OutboxInput, StoredOp};

const DISCARD: u64 = 77;

fn answered(twin: &mut Twin, req_id: RequestId, pending_retained: bool) {
    twin.post(OutboxInput::Answered { req_id, outcome: AuthorOutcome::Error { class: ErrorClass::Network, error: SessionError::Http { status: 0 }, pending_retained } });
}

fn queued_after_answer(pending_retained: bool) -> (Twin, OpId, RequestId) {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let req_id = twin.first_pair[&id].0;
    answered(&mut twin, req_id, pending_retained);
    twin.post(OutboxInput::Online(Online(false)));
    twin.post(OutboxInput::RetryDue { op_id: id, cycle_id: 1 });
    (twin, id, req_id)
}

fn discard(twin: &mut Twin) { twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: DISCARD }); }

fn releases(sends: &[SessionInput]) -> Vec<RequestId> { sends.iter().filter_map(|input| match input { SessionInput::Release { req_id } => Some(*req_id), _ => None }).collect() }

#[test]
fn out_discard_releases_pending() {
    let (mut twin, id, req_id) = queued_after_answer(true);
    discard(&mut twin);
    assert!(twin.store_ops.contains(&StoreOp::ReleasePendingOwner { request_id: req_id }), "the rollback batch hands the pair to Session: {:?}", twin.store_ops);
    assert_eq!(releases(&twin.session_sends), Vec::<RequestId>::new(), "Release waits for the commit of the rollback");
    twin.committed(id);
    assert_eq!(releases(&twin.session_sends), vec![req_id], "{:?}", twin.session_sends);
    twin.post(OutboxInput::Online(Online(true)));
    assert_eq!(releases(&twin.session_sends), Vec::<RequestId>::new(), "Release goes out once");
}

#[test]
fn out_discard_without_pending_owner() {
    let (mut twin, id, _) = queued_after_answer(false);
    discard(&mut twin);
    assert!(!has_op!(twin, StoreOp::ReleasePendingOwner { .. }), "{:?}", twin.store_ops);
    twin.committed(id);
    assert_eq!(releases(&twin.session_sends), Vec::<RequestId>::new(), "no pending, nothing to release");
}

#[test]
fn out_discard_pending_owner_crash() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    twin.post(OutboxInput::Online(Online(false)));
    let restored = StoredOp { answered: None, op_id: 7, op: model_op("r", Policy::Rollback, false), created_at: 0, error_mark: false, pending: Some((41, 42)), attempts: 1, cycle_id: 1, cause: None, restore_failed: None, restore_element: None, removed_rows: Vec::new() };
    twin.post(OutboxInput::Restored { ops: vec![restored], pending_lost: false });
    twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
    discard(&mut twin);
    assert!(twin.store_ops.contains(&StoreOp::ReleasePendingOwner { request_id: 41 }), "a pair restored from the base is released by the rollback batch: {:?}", twin.store_ops);
    twin.rejected(7, super::outbox_harness::RReason::Storage);
    assert!(twin.state().ops[&7].pending_owned, "a refused batch leaves the owner as it was");
    assert_eq!(releases(&twin.session_sends), Vec::<RequestId>::new(), "no Release without the commit");
}

fn retried_request_id(pending_retained: bool) -> (RequestId, RequestId) {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Keep, false);
    twin.committed(id);
    let first = twin.first_pair[&id].0;
    twin.post(OutboxInput::Answered { req_id: first, outcome: AuthorOutcome::Error { class: ErrorClass::Permanent, error: SessionError::Http { status: 0 }, pending_retained } });
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Failed));
    super::outbox_cells_tests::retry(&mut twin);
    let effects = twin.committed(id);
    let second = effects.iter().find_map(|effect| match effect { OutboxEffect::Send { request_id, .. } => Some(*request_id), _ => None }).expect("the retry sends");
    (first, second)
}

#[test]
fn out_retry_request_identity() {
    let (kept_first, kept_second) = retried_request_id(true);
    assert_eq!(kept_second, kept_first, "an unresolved pending keeps the request id");
    let (released_first, released_second) = retried_request_id(false);
    assert_ne!(released_second, released_first, "a released pending starts the cycle with a new request id");
}
