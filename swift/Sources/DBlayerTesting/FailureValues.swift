import DBlayer

private func wireFailure(kind: String, reason: String?, message: String) -> WireFailure {
  WireFailure(kind: kind, sessionCode: nil, requestSize: nil, requestLimit: nil, message: message, extensions: nil, responseLimit: nil, reason: reason)
}

private func wireState(_ state: SyncState) -> WireSyncState {
  switch state {
  case .synced: .synced
  case .pending: .pending
  case .sending: .sending
  case .failed: .failed
  }
}

private func wireStatus(_ status: UploadStatus) -> WireUploadStatus {
  switch status {
  case .waiting: .waiting
  case .preparing: .preparing
  case .queued: .queued
  case .uploading: .uploading
  case .sending: .sending
  case .done: .done
  case .failed: .failed
  case .cancelled: .cancelled
  }
}

/// A refusal as a row reads it, the other fields are empty.
public func operationFailure(kind: String, reason: String? = nil, message: String = "") -> OperationFailure {
  syncInfo(state: .failed, kind: kind, reason: reason, message: message).error!
}

/// The sync value of a row: the state and the refusal of its last operation, if any.
public func syncInfo(state: SyncState, error: OperationFailure? = nil) -> SyncInfo {
  syncInfo(state: state, kind: error?.kind, reason: error?.reason, message: error?.message ?? "")
}

/// The value of `uploads[action]`: the status, the sent share from 0 to 1 and the refusal.
public func uploadInfo(_ status: UploadStatus, progress: Double = 0, error: OperationFailure? = nil) -> UploadInfo {
  UploadInfo(wire: WireUpload(
    status: wireStatus(status), stage: "", sentBytes: 0, totalBytes: 0, progress: progress, waitReason: nil, attempts: 0,
    error: error.map { wireFailure(kind: $0.kind, reason: $0.reason, message: $0.message) }, localUrl: nil, processing: nil
  ))
}

private func syncInfo(state: SyncState, kind: String?, reason: String?, message: String) -> SyncInfo {
  SyncInfo(wire: WireSync(
    state: wireState(state), actions: [], dirtyFields: [], error: kind.map { wireFailure(kind: $0, reason: reason, message: message) }, attempts: 0, since: 0
  ))
}
