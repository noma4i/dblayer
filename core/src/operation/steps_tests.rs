use super::*;
use crate::schema::RootShape;
use OperationStatus::*;
use RequestActionStep::*;

#[test]
fn request_steps_skip_envelope_a_only_for_plain_actions() {
    let all = [EncodeInput, CheckOnceKey, PlanOptimistic, CommitEnvelopeA, PrepareTransport, CallTransport, CommitEnvelopeB, RunInvalidations, Track];
    assert_eq!(RequestActionStep::ALL, all);
    assert_eq!(
        request_steps(false, false),
        [EncodeInput, CheckOnceKey, PlanOptimistic, PrepareTransport, CallTransport, CommitEnvelopeB, RunInvalidations, Track]
    );
    assert_eq!(request_steps(false, true), all);
    assert_eq!(request_steps(true, false), all);
}

#[test]
fn outcome_decides_status_and_envelope_b() {
    assert_eq!(status_for_outcome(TransportOutcome::Data), Committed);
    assert_eq!(status_for_outcome(TransportOutcome::DataWithErrors), Failed);
    assert_eq!(status_for_outcome(TransportOutcome::ThrownError), DeliveryUnknown);
    assert!(commits_envelope_b(TransportOutcome::Data));
    assert!(!commits_envelope_b(TransportOutcome::DataWithErrors));
    assert!(!commits_envelope_b(TransportOutcome::ThrownError));
}

#[test]
fn generation_check_starts_at_transport_and_superseded_aborts() {
    assert!(!checks_generation(PlanOptimistic));
    assert!(!checks_generation(CommitEnvelopeA));
    assert!(checks_generation(PrepareTransport));
    assert!(checks_generation(CallTransport));
    assert!(!aborts_without_write(false));
    assert!(aborts_without_write(true));
}

#[test]
fn durable_phases_run_their_steps_and_root_accepts_upsert_only() {
    assert_eq!(durable_steps(DurablePhase::Start), [EncodeInput, PlanOptimistic, CommitEnvelopeA]);
    assert_eq!(durable_steps(DurablePhase::Execute), [PrepareTransport, CallTransport, CommitEnvelopeB, RunInvalidations, Track]);
    assert_eq!(durable_steps(DurablePhase::Cancel), [CommitEnvelopeA]);
    assert!(durable_accepts_root(RootShape::Upsert));
    assert!(!durable_accepts_root(RootShape::Destroy));
}

#[test]
fn resume_order_blocked_rows_and_manual_retry() {
    let operations = [
        ResumeCandidate::new(4, Pending, false),
        ResumeCandidate::new(1, Pending, false),
        ResumeCandidate::new(3, Pending, true),
        ResumeCandidate::new(2, Committed, false),
    ];
    assert_eq!(resume_order(&operations), [1, 4]);
    assert!(blocks_row(Pending, true));
    assert!(!blocks_row(Pending, false));
    assert!(!blocks_row(Committed, true));
    assert!(!awaits_manual_retry(Pending));
    assert!(awaits_manual_retry(Failed));
    assert!(!awaits_manual_retry(DeliveryUnknown));
}

#[test]
fn durable_errors_use_local_trace_to_choose_failure_status() {
    assert_eq!(durable_next(true, 1, false, true), Failed);
    assert_eq!(durable_next(true, 1, false, false), RolledBack);
    assert_eq!(durable_next(true, 0, false, false), Committed);
    assert_eq!(durable_next(false, 0, false, false), DeliveryUnknown);
    assert_eq!(durable_next(true, 0, true, false), DeliveryUnknown);
}

#[test]
fn durable_retry_and_temp_row_rules_match_statuses() {
    assert!(!durable_allows_retry(Pending));
    assert!(!durable_allows_retry(DeliveryUnknown));
    assert!(durable_allows_retry(Failed));
    assert!(!durable_allows_retry(Committed));
    assert!(!durable_allows_retry(RolledBack));
    assert!(!keeps_temp_row(Committed));
    assert!(!keeps_temp_row(RolledBack));
    assert!(keeps_temp_row(Failed));
}

#[test]
fn failure_status_follows_temp_id_rollback_and_unknown_delivery() {
    assert_eq!(failure_status(true, false, false), Failed);
    assert_eq!(failure_status(false, false, false), RolledBack);
    assert_eq!(failure_status(false, true, false), Failed);
    assert_eq!(failure_status(false, false, true), DeliveryUnknown);
    assert_eq!(failure_status(true, true, true), DeliveryUnknown);
}

#[test]
fn failure_ledger_retention_retry_and_badge_follow_status() {
    for status in ALL_STATUSES {
        assert_eq!(failure_allows_retry(status), status == Failed);
        assert_eq!(keeps_ledger_row(status), [Pending, Failed, DeliveryUnknown].contains(&status));
    }
    assert!(shows_badge(DeliveryUnknown, false));
    assert!(!shows_badge(DeliveryUnknown, true));
    assert!(!shows_badge(Failed, false));
}
