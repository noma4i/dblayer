use crate::owners::write::operation_status::OperationStatus;
use crate::value::sync::{FailureKind, OperationFailure};

use super::*;

fn state(phase: UploadStatus, sent: u64, total: u64) -> UploadState {
    UploadState { phase, sent_bytes: sent, total_bytes: total, ..UploadState::queued() }
}

#[test]
fn status_follows_the_operation_before_the_phase() {
    let uploading = state(UploadStatus::Uploading, 5, 10);
    let show = |status, started| UploadInfo::of(status, started, &uploading, None, None).status;
    assert_eq!(show(OperationStatus::Pending, false), UploadStatus::Uploading);
    assert_eq!(show(OperationStatus::Pending, true), UploadStatus::Sending);
    assert_eq!(show(OperationStatus::Failed, false), UploadStatus::Failed);
}

#[test]
fn fraction_is_bytes_over_total_and_zero_without_a_total() {
    let info = |sent, total| UploadInfo::of(OperationStatus::Pending, false, &state(UploadStatus::Uploading, sent, total), None, None);
    assert_eq!(info(5, 10).fraction(), 0.5);
    assert_eq!(info(0, 0).fraction(), 0.0);
    assert_eq!(info(20, 10).fraction(), 1.0);
}

#[test]
fn only_a_rejection_is_not_retryable() {
    let failed = |kind| UploadInfo::of(OperationStatus::Failed, false, &UploadState::queued(), None, Some(OperationFailure::new(kind, "x")));
    assert!(failed(FailureKind::Network).is_retryable());
    assert!(!failed(FailureKind::Rejected).is_retryable());
}

#[test]
fn a_pending_upload_with_a_wait_reason_shows_waiting_and_carries_the_reason_only_then() {
    let queued = UploadState::queued();
    let waiting = UploadInfo::of(OperationStatus::Pending, false, &queued, Some(WaitReason::CreateUnconfirmed), None);
    assert_eq!((waiting.status, waiting.wait_reason), (UploadStatus::Waiting, Some(WaitReason::CreateUnconfirmed)));
    let sending = UploadInfo::of(OperationStatus::Pending, true, &queued, Some(WaitReason::CreateUnconfirmed), None);
    assert_eq!((sending.status, sending.wait_reason), (UploadStatus::Sending, None));
    let free = UploadInfo::of(OperationStatus::Pending, false, &queued, None, None);
    assert_eq!((free.status, free.wait_reason), (UploadStatus::Queued, None));
}
