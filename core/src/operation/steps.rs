use super::status::{is_open, recovered_status, removes_row, OperationStatus};
use crate::schema::RootShape;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestActionStep {
    EncodeInput,
    CheckOnceKey,
    PlanOptimistic,
    CommitEnvelopeA,
    PrepareTransport,
    CallTransport,
    CommitEnvelopeB,
    RunInvalidations,
    Track,
}

impl RequestActionStep {
    pub const ALL: [RequestActionStep; 9] = [
        RequestActionStep::EncodeInput,
        RequestActionStep::CheckOnceKey,
        RequestActionStep::PlanOptimistic,
        RequestActionStep::CommitEnvelopeA,
        RequestActionStep::PrepareTransport,
        RequestActionStep::CallTransport,
        RequestActionStep::CommitEnvelopeB,
        RequestActionStep::RunInvalidations,
        RequestActionStep::Track,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportOutcome {
    Data,
    DataWithErrors,
    ThrownError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurablePhase {
    Start,
    Execute,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeCandidate {
    pub sequence: i64,
    pub status: OperationStatus,
    pub transport_started: bool,
}

impl ResumeCandidate {
    pub fn new(sequence: i64, status: OperationStatus, transport_started: bool) -> Self {
        Self { sequence, status, transport_started }
    }
}

pub fn request_steps(has_optimistic: bool, has_once_key: bool) -> Vec<RequestActionStep> {
    let keeps_envelope_a = has_optimistic || has_once_key;
    RequestActionStep::ALL
        .into_iter()
        .filter(|step| keeps_envelope_a || *step != RequestActionStep::CommitEnvelopeA)
        .collect()
}

pub fn status_for_outcome(outcome: TransportOutcome) -> OperationStatus {
    match outcome {
        TransportOutcome::Data => OperationStatus::Committed,
        TransportOutcome::DataWithErrors => OperationStatus::Failed,
        TransportOutcome::ThrownError => OperationStatus::DeliveryUnknown,
    }
}

pub fn commits_envelope_b(outcome: TransportOutcome) -> bool {
    outcome == TransportOutcome::Data
}

pub fn checks_generation(step: RequestActionStep) -> bool {
    !matches!(
        step,
        RequestActionStep::EncodeInput
            | RequestActionStep::CheckOnceKey
            | RequestActionStep::PlanOptimistic
            | RequestActionStep::CommitEnvelopeA
    )
}

pub fn aborts_without_write(is_superseded: bool) -> bool {
    is_superseded
}

pub fn durable_steps(phase: DurablePhase) -> Vec<RequestActionStep> {
    use RequestActionStep::*;
    match phase {
        DurablePhase::Start => vec![EncodeInput, PlanOptimistic, CommitEnvelopeA],
        DurablePhase::Execute => vec![PrepareTransport, CallTransport, CommitEnvelopeB, RunInvalidations, Track],
        DurablePhase::Cancel => vec![CommitEnvelopeA],
    }
}

pub fn durable_accepts_root(shape: RootShape) -> bool {
    shape == RootShape::Upsert
}

pub fn resume_order(operations: &[ResumeCandidate]) -> Vec<i64> {
    let mut sequences: Vec<i64> = operations
        .iter()
        .filter(|candidate| candidate.status == OperationStatus::Pending && !candidate.transport_started)
        .map(|candidate| candidate.sequence)
        .collect();
    sequences.sort_unstable();
    sequences
}

pub fn blocks_row(status: OperationStatus, transport_started: bool) -> bool {
    recovered_status(transport_started, status) == OperationStatus::DeliveryUnknown
}

pub fn awaits_manual_retry(status: OperationStatus) -> bool {
    status == OperationStatus::Failed
}

pub fn durable_next(has_data: bool, error_count: usize, transport_threw: bool, has_local_trace: bool) -> OperationStatus {
    if transport_threw {
        return OperationStatus::DeliveryUnknown;
    }
    if error_count > 0 {
        return if has_local_trace { OperationStatus::Failed } else { OperationStatus::RolledBack };
    }
    if has_data {
        OperationStatus::Committed
    } else {
        OperationStatus::DeliveryUnknown
    }
}

pub fn durable_allows_retry(status: OperationStatus) -> bool {
    status == OperationStatus::Failed
}

pub fn keeps_temp_row(status: OperationStatus) -> bool {
    !removes_row(status)
}

pub fn failure_status(has_temp_id: bool, has_rollback: bool, delivery_unknown: bool) -> OperationStatus {
    if delivery_unknown {
        OperationStatus::DeliveryUnknown
    } else if has_temp_id || has_rollback {
        OperationStatus::Failed
    } else {
        OperationStatus::RolledBack
    }
}

pub fn keeps_ledger_row(status: OperationStatus) -> bool {
    is_open(status)
}

pub fn failure_allows_retry(status: OperationStatus) -> bool {
    status == OperationStatus::Failed
}

pub fn shows_badge(status: OperationStatus, dismissed: bool) -> bool {
    status == OperationStatus::DeliveryUnknown && !dismissed
}
