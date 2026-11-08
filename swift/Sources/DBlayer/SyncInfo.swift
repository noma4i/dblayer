import Foundation

public enum SyncState: String, Codable, Sendable { case synced, pending, sending, failed }

public struct OperationFailure: Codable, Sendable, Equatable {
  public let kind: String
  public let message: String
  public let extensions: GraphqlExtensions?
}

public struct SyncInfo: Codable, Sendable, Equatable {
  public let state: SyncState
  public let actions: [String]
  public let dirtyFields: [String]
  public let error: OperationFailure?
  public let attempts: Int
  public let since: Int64

  public init() { self.init(wire: defaultSync()) }

  init(state: SyncState, actions: [String], dirtyFields: [String], error: OperationFailure?, attempts: Int, since: Int64) {
    self.state = state
    self.actions = actions
    self.dirtyFields = dirtyFields
    self.error = error
    self.attempts = attempts
    self.since = since
  }
}

public struct UploadProcessing: Codable, Sendable, Equatable {
  public let progress: Double?
}

public enum UploadStatus: String, Codable, Sendable { case waiting, preparing, queued, uploading, sending, done, failed, cancelled }

public struct UploadInfo: Codable, Sendable, Equatable {
  public let status: UploadStatus
  public let stage: String
  public let sentBytes: UInt64
  public let totalBytes: UInt64
  /// The sent share of the bytes in percent, 0 to 100.
  public let progress: Double
  public let waitReason: String?
  /// Sum of retries for unfinished steps, preserved across restarts.
  public let attempts: Int
  public let error: OperationFailure?
  /// The file URL of the copy of the library while the transfer holds it.
  public let localUrl: String?
  /// The server works on the file after the transfer: the progress it reports.
  public let processing: UploadProcessing?

  enum CodingKeys: String, CodingKey {
    case status, stage, attempts, error, processing
    case progress
    case localUrl = "local_url"
    case sentBytes = "sent_bytes", totalBytes = "total_bytes", waitReason = "wait_reason"
  }
}

extension SyncState {
  init(wire: WireSyncState) {
    switch wire {
    case .synced: self = .synced
    case .pending: self = .pending
    case .sending: self = .sending
    case .failed: self = .failed
    }
  }
}

extension UploadStatus {
  init(wire: WireUploadStatus) {
    switch wire {
    case .waiting: self = .waiting
    case .preparing: self = .preparing
    case .queued: self = .queued
    case .uploading: self = .uploading
    case .sending: self = .sending
    case .done: self = .done
    case .failed: self = .failed
    case .cancelled: self = .cancelled
    }
  }
}

extension OperationFailure {
  init(wire: WireFailure) { self.init(kind: wire.kind, message: wire.message, extensions: wire.extensions) }
}

extension SyncInfo {
  public init(wire: WireSync) {
    self.init(state: SyncState(wire: wire.state), actions: wire.actions, dirtyFields: wire.dirtyFields, error: wire.error.map(OperationFailure.init(wire:)), attempts: Int(wire.attempts), since: wire.since)
  }
}

extension UploadInfo {
  public init(wire: WireUpload) {
    self.init(
      status: UploadStatus(wire: wire.status), stage: wire.stage, sentBytes: wire.sentBytes, totalBytes: wire.totalBytes, progress: wire.progress,
      waitReason: wire.waitReason, attempts: Int(wire.attempts), error: wire.error.map(OperationFailure.init(wire:)),
      localUrl: wire.localUrl, processing: wire.processing.map { UploadProcessing(progress: $0.progress) }
    )
  }
}
