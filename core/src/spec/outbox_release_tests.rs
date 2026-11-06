//! The release of an unresolved pending by a discard (`outbox.md`: `pending_owned`, RollingBack x Committed by Discard).

use super::outbox_harness::*;
use super::ref_outbox::Policy;
use crate::owners::contract::{ErrorClass, Online, RequestId};
use crate::owners::session::{AuthorOutcome, SessionError};
use crate::owners::write::outbox::{OpId, OutboxInput, StoredOp};

const DISCARD: u64 = 77;

fn answered(twin: &mut Twin, req_id: RequestId, pending_retained: bool) {
    twin.post(OutboxInput::Answered { req_id, outcome: AuthorOutcome::Error { class: ErrorClass::Network, error: SessionError::Http { status: 0 }, pending_retained } });
}

/// The operation sent once, its answer a temporary error, then queued while offline.
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

fn discard(twin: &mut Twin) { twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: DISCARD }); }

fn releases(text: &str) -> usize { text.matches("Release {").count() }

#[test]
fn out_discard_releases_pending() {
    let (mut twin, id, req_id) = queued_after_answer(true);
    discard(&mut twin);
    assert!(twin.store_text.contains(&format!("ReleasePendingOwner {{ request_id: {req_id} }}")), "the rollback batch hands the pair to Session: {}", twin.store_text);
    assert_eq!(releases(&twin.session_text), 0, "Release waits for the commit of the rollback");
    twin.committed(id);
    assert_eq!(releases(&twin.session_text), 1, "{}", twin.session_text);
    assert!(twin.session_text.contains(&format!("req_id: {req_id}")));
    twin.post(OutboxInput::Online(Online(true)));
    assert_eq!(releases(&twin.session_text), 0, "Release goes out once");
}

#[test]
fn out_discard_without_pending_owner() {
    let (mut twin, id, _) = queued_after_answer(false);
    discard(&mut twin);
    assert!(!twin.store_text.contains("ReleasePendingOwner"), "{}", twin.store_text);
    twin.committed(id);
    assert_eq!(releases(&twin.session_text), 0, "no pending, nothing to release");
}

#[test]
fn out_discard_pending_owner_crash() {
    let mut twin = Twin::bare();
    twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
    twin.post(OutboxInput::Online(Online(false)));
    let restored = StoredOp { answered: None, op_id: 7, op: model_op("r", Policy::Rollback, false), error_mark: false, pending: Some((41, 42)), attempts: 1, cycle_id: 1, restore_failed: None, restore_element: None };
    twin.post(OutboxInput::Restored { ops: vec![restored], pending_lost: false });
    twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
    discard(&mut twin);
    assert!(twin.store_text.contains("ReleasePendingOwner { request_id: 41 }"), "a pair restored from the base is released by the rollback batch: {}", twin.store_text);
    twin.rejected(7, super::ref_outbox::Reason::Storage);
    assert!(twin.state().ops[&7].pending_owned, "a refused batch leaves the owner as it was");
    assert_eq!(releases(&twin.session_text), 0, "no Release without the commit");
}
