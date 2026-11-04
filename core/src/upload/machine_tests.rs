use crate::operation::OperationStatus;
use crate::syncstate::{FailureKind, OperationFailure};

use super::*;

fn failure() -> OperationFailure {
    OperationFailure::new(FailureKind::Network, "x")
}

fn failed(batch: bool, resume_at: Option<UploadStage>) -> UploadEvent {
    UploadEvent::Failed { failure: failure(), at: 9, batch, resume_at }
}

#[test]
fn a_late_transfer_event_is_refused_after_cancel_and_changes_nothing() {
    let mut state = UploadState::queued();
    state.apply(UploadEvent::Cancel).unwrap();
    let before = state.clone();
    assert_eq!(state.apply(UploadEvent::Progress { sent: 5 }), Err(Refused { phase: UploadStatus::Cancelled }));
    assert_eq!(state, before);
    state.apply(UploadEvent::AbortStarted).unwrap();
    assert_eq!((state.phase, state.stage), (UploadStatus::Cancelled, UploadStage::Abort));
}

#[test]
fn only_a_failed_upload_is_retried_and_retry_clears_its_failure() {
    let mut state = UploadState::queued();
    assert!(state.apply(UploadEvent::Retried).is_err());
    state.apply(failed(true, None)).unwrap();
    state.retries.push(UploadRetry { stage: UploadStage::Put, part_number: 0, attempts: 2, retry_at: None });
    assert_eq!((state.phase, state.failed_at), (UploadStatus::Failed, Some(9)));
    state.apply(UploadEvent::Retried).unwrap();
    assert_eq!((state.phase, state.error.is_none(), state.failed_at, state.retries.len()), (UploadStatus::Queued, true, None, 0));
}

#[test]
fn a_single_failure_keeps_the_phase_and_restores_the_resume_stage() {
    let mut state = UploadState::queued();
    state.apply(UploadEvent::Start).unwrap();
    state.apply(UploadEvent::Located { total: 10, scenario: UploadScenario::Single }).unwrap();
    state.apply(UploadEvent::PutStarted).unwrap();
    state.apply(UploadEvent::AbortStarted).unwrap();
    state.apply(failed(false, Some(UploadStage::Put))).unwrap();
    assert_eq!((state.phase, state.stage, state.error.is_some()), (UploadStatus::Uploading, UploadStage::Put, true));
}

#[test]
fn the_mutation_starts_only_from_a_phase_that_can_send() {
    let request = UploadRequest { document: "d".into(), name: "n".into(), variables: serde_json::Value::Null };
    let mut state = UploadState::queued();
    state.apply(failed(true, None)).unwrap();
    assert!(state.apply(UploadEvent::MutationBegan { request: request.clone() }).is_err());
    let mut state = UploadState::queued();
    state.apply(UploadEvent::MutationBegan { request }).unwrap();
    assert_eq!((state.phase, state.stage), (UploadStatus::Sending, UploadStage::Mutation));
}

#[test]
fn the_run_follows_the_operation_and_the_stored_upload() {
    let pending = |state: &UploadState, started, waiting| state.run_mode(OperationStatus::Pending, started, waiting, || false);
    let queued = UploadState::queued();
    let transferred = UploadState { transferred: true, ..UploadState::queued() };
    assert_eq!(pending(&queued, false, false), Some(RunMode::Transfer));
    assert_eq!(pending(&transferred, false, false), Some(RunMode::Mutation));
    assert_eq!(pending(&queued, true, false), None);
    assert_eq!(pending(&queued, false, true), None);
    let cancelled = UploadState { phase: UploadStatus::Cancelled, ..UploadState::queued() };
    assert_eq!(pending(&cancelled, true, true), Some(RunMode::Cancel));
    let orphan = UploadState { phase: UploadStatus::Failed, upload_id: Some("u".into()), key: Some("k".into()), ..UploadState::queued() };
    assert_eq!(pending(&orphan, false, false), Some(RunMode::Abort));
    let confirmed = UploadState { scenario: Some(UploadScenario::Multipart), total_bytes: 5, parts: vec![UploadPart { number: 1, etag: "e".into(), confirmed_bytes: 5 }], ..orphan.clone() };
    assert_eq!(pending(&confirmed, false, false), None);
    assert_eq!(orphan.run_mode(OperationStatus::Failed, false, false, || true), None);
    assert_eq!(orphan.run_mode(OperationStatus::DeliveryUnknown, true, false, || false), Some(RunMode::Abort));
}
