//! Owner-level tests of the `outbox` machine: the `Sending` failures, the `Landing` commit and the rollbacks. Names are the `Test` column.

use super::outbox_cells_tests::*;
use super::outbox_harness::*;
use super::outbox_harness::{Policy, RWhy, St};
use crate::owners::contract::{Online, RejectReason};
use crate::owners::write::outbox::{CommitKind, Fault, NewOp, OutboxAnswer, OutboxInput, OutboxOutcome};

#[test]
fn out_submitting_rejected() {
    for reason in [RejectReason::NoSession, RejectReason::Closed, RejectReason::Storage("disk".into()), RejectReason::Invalid] {
        let mut twin = Twin::new();
        let id = twin.submit("r", Policy::Rollback, false);
        let effects = refuse(&mut twin, id, reason.clone());
        assert_eq!((twin.stage(id), answers(&effects, SUBMIT_REPLY)), (Some(St::Done), vec![OutboxAnswer::Err(Fault::Rejected(reason))]), "the submit is refused with the reason, no optimism");
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
fn out_sending_tick_offline() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    twin.post(OutboxInput::Online(Online(false)));
    let effects = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&effects), sends(&effects)), (Some(St::Queued), vec![CommitKind::Requeue], 0), "a retry tick while offline requeues with the pending status");
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
            assert!(twin.store_text.contains(&format!("MarkError {{ op_id: {id}, error: Some(")), "{}", twin.store_text);
            let expected = twin.state().ops[&id].error.as_ref().unwrap();
            if cause.is_some() { assert_eq!(serde_json::from_str::<serde_json::Value>(expected).unwrap()["kind"], "response_too_large:64"); }
            else { assert_eq!(expected, "Exhausted"); }
            assert!(twin.store_text.contains(&format!("{expected:?}")), "{}", twin.store_text);
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
    let db = crate::store::Store::open_models(None, vec![crate::schema_fixtures::moments_spec(), crate::schema_fixtures::users_spec()], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let job = DecodeJob { op_id: id, model: "Moment".into(), action: "destroy".into(), temp: None, batch: None, body: r#"{"errors":[{"message":"missing","extensions":{"code":"NOT_FOUND"}}]}"#.into() };
    let Decoded::MissingDestroy { stale } = decode_answer(&db, &|_| None, &job) else { panic!("NOT_FOUND destroy must land") };
    twin.post(OutboxInput::MissingDestroy { op_id: id, stale });
    assert_eq!(twin.stores.len(), 1);
    assert!(twin.store_text.contains("RemoveServerRows { model: \"Moment\", ids: [\"r\"] }"), "{}", twin.store_text);
    assert!(twin.store_text.contains(&format!("RemoveIntent {{ op_id: {id} }}")) && twin.store_text.contains(&format!("RemoveAnswer {{ request_id: {request_id} }}")), "{}", twin.store_text);
}

#[test]
fn out_sending_destroy_not_found_optimistic() {
    let mut twin = Twin::new();
    let effects = crate::owners::store::IntentEffects { tombstones: vec![crate::owners::store::EffectTombstone { model: "Moment".into(), row_id: "r".into() }], ..Default::default() };
    let op = NewOp { action: "destroy".into(), effects, ..model_op("r", Policy::Rollback, false) };
    let id = submit_op(&mut twin, op, 30);
    twin.committed(id);
    twin.post(OutboxInput::MissingDestroy { op_id: id, stale: Vec::new() });
    assert_eq!(twin.stores.len(), 1);
    assert!(twin.store_text.contains("RemoveServerRows { model: \"Moment\", ids: [\"r\"] }"), "{}", twin.store_text);
}

#[test]
fn out_sending_destroy_not_found() {
    let mut twin = Twin::new();
    let op = NewOp { action: "destroy".into(), ..model_op("r", Policy::Rollback, false) };
    let id = submit_op(&mut twin, op, 30);
    twin.committed(id);
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!(twin.stage(id), Some(St::Landing), "the not-found destroy lands: the server row is already gone");
    let effects = twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Done));
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))), "{effects:?}");
}

#[test]
fn out_sending_other() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    unchanged_by(&mut twin, id, St::Sending, vec![
        OutboxInput::Committed(crate::owners::contract::Committed { batch: 999, revision: 50, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }),
        OutboxInput::Rejected(crate::owners::contract::Rejected { batch: 999, reason: RejectReason::Storage("disk".into()) }),
    ]);
}

#[test]
fn out_landing_committed() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let follower = submit_op(&mut twin, model_op("r", Policy::Rollback, false), 30);
    twin.committed(follower);
    assert_eq!(twin.stage(follower), Some(St::Queued), "the next operation of the row waits behind the sending one");
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = twin.committed(id);
    assert_eq!((twin.stage(id), twin.stage(follower), sends(&effects)), (Some(St::Done), Some(St::Sending), 1), "the operation is Done and the next one of the row starts");
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))), "{effects:?}");
}

#[test]
fn out_landing_committed_transfer() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, true);
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
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

#[test]
fn out_rollback_committed_discard() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Queued));
    twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
    assert_eq!(twin.stage(id), Some(St::RollingBack(RWhy::Discarded)));
    let effects = twin.committed(id);
    assert_eq!((twin.stage(id), answers(&effects, DISCARD_REPLY)), (Some(St::Done), vec![OutboxAnswer::Ok]), "the discard is answered Ok after the rollback");
    assert_eq!(twin.state().ops[&id].outcome, Some(OutboxOutcome::Discarded));
}
