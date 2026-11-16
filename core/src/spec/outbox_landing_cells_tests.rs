//! Owner-level tests of the `outbox` machine: the `Sending` failures, the `Landing` commit and the rollbacks. Names are the `Test` column.

use super::outbox_cells_tests::*;
use super::outbox_harness::*;
use super::outbox_harness::{Policy, RWhy, St};
use crate::owners::contract::{Online, RejectReason};
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{CommitKind, NewOp, OutboxAnswer, OutboxInput, OutboxOutcome};

#[test]
fn out_submitting_rejected() {
    for reason in [RejectReason::NoSession, RejectReason::Closed, RejectReason::Storage("disk".into()), RejectReason::Invalid(String::new())] {
        let mut twin = Twin::new();
        let id = twin.submit("r", Policy::Rollback, false);
        let effects = refuse(&mut twin, id, reason.clone());
        assert_eq!((answers(&effects, SUBMIT_REPLY), kinds(&effects)), (vec![OutboxAnswer::Err(crate::spec::support::refusal_of(reason))], Vec::new()), "the submit is refused with the reason, no optimism, no batch follows");
    }
}

#[test]
fn out_sending_temp_offline() {
    for dropped in [false, true] {
        let mut twin = Twin::new();
        let id = sending(&mut twin, Policy::Rollback, false);
        twin.post(OutboxInput::Online(Online(false)));
        let input = if dropped { OutboxInput::Dropped { op_id: id } } else { OutboxInput::TransportError { op_id: id, temporary: true } };
        let effects = twin.post(input);
        assert_eq!((twin.stage(id), kinds(&effects), sends(&effects), timers(&effects)), (Some(St::Queued), vec![CommitKind::Requeue], 0, 0), "offline: back to Queued with the pending status, no request and no timer");
        assert_eq!(twin.state().ops[&id].attempts, 1, "the attempt counter is kept");
    }
}

#[test]
fn out_sending_exhausted_keep() {
    for policy in [Policy::Keep, Policy::Rollback] {
        for cause in [None, Some(64)] {
            let mut twin = Twin::new();
            let id = exhausted(&mut twin, policy);
            let input = if let Some(limit) = cause {
                OutboxInput::Answered { req_id: twin.state().ops[&id].request_id, outcome: crate::owners::session::AuthorOutcome::Error { class: crate::owners::contract::ErrorClass::Network, error: crate::owners::session::SessionError::ResponseTooLarge { limit }, pending_retained: true } }
            } else { OutboxInput::Dropped { op_id: id } };
            let effects = twin.post(input);
            assert_exhausted_mark(&twin, id, cause.is_some());
            let expected_kinds = if cause.is_some() { vec![CommitKind::Land, CommitKind::ErrorMark] } else { vec![CommitKind::ErrorMark] };
            assert_eq!((twin.stage(id), kinds(&effects), sends(&effects)), (Some(St::MarkingFailed), expected_kinds, 0), "{policy:?} cause={cause:?}: the 40th attempt fails under any on_failure");
        }
    }
}

#[test]
fn out_sending_destroy_not_found_batch() {
    use crate::owners::write::answer::{decode_answer, DecodeJob, Decoded};
    let mut twin = Twin::new();
    let op = NewOp { action: "destroy".into(), ..model_op("r", Policy::Rollback, false) };
    let id = submit_op(&mut twin, op, 30);
    twin.committed(id);
    let request_id = twin.state().ops[&id].request_id;
    let db = crate::store::Store::open_models(None, vec![crate::schema_fixtures::moments_spec(), crate::schema_fixtures::users_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let job = DecodeJob { op_id: id, model: "Moment".into(), action: "destroy".into(), temp: None, batch: None, body: r#"{"errors":[{"message":"missing","extensions":{"code":"NOT_FOUND"}}]}"#.into() };
    let Decoded::MissingDestroy { stale } = decode_answer(&db, &|_| None, &job) else { panic!("NOT_FOUND destroy must land") };
    twin.post(OutboxInput::MissingDestroy { op_id: id, stale });
    assert_eq!(twin.stores.len(), 1);
    assert_eq!(twin.batch_ops(), vec![StoreOp::RemoveServerRows { model: "Moment".into(), ids: vec!["r".into()] }, StoreOp::RemoveIntent { op_id: id }, StoreOp::RemoveAnswer { request_id }]);
}

#[test]
fn out_landing_committed() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let follower = submit_op(&mut twin, model_op("r", Policy::Rollback, false), 30);
    twin.committed(follower);
    assert_eq!(twin.stage(follower), Some(St::Queued), "the next operation of the row waits behind the sending one");
    let waited = twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH), reply: WAITER, discard_on_failure: false });
    assert_eq!((twin.stage(id), waited.len()), (Some(St::Sending), 0));
    twin.respond(id);
    let effects = twin.committed(id);
    assert_eq!((twin.stage(id), twin.stage(follower), sends(&effects)), (Some(St::Done), Some(St::Sending), 1), "the operation is Done and the next one of the row starts");
    assert!(matches!(answers(&effects, WAITER).as_slice(), [OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. })]), "{effects:?}");
}

#[test]
fn out_landing_committed_transfer() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, true);
    twin.waiter(id);
    twin.respond(id);
    let effects = twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Done));
    assert!(matches!(transfer_outcomes(&effects).as_slice(), [OutboxOutcome::Confirmed { .. }]), "the outcome reaches Transfer in the batch that removes the intent: {effects:?}");
}

#[test]
fn out_rollback_committed() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.waiter(id);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    assert_eq!(twin.stage(id), Some(St::RollingBack(RWhy::Rejected)));
    let effects = twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Done));
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Rejected { .. }))), "the rollback gives Rejected, no optimism: {effects:?}");
}

