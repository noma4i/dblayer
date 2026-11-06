import Foundation

public enum SyncState: String, Codable, Sendable { case synced, pending, sending, failed }

public struct OperationFailure: Codable, Sendable, Equatable {
  public let kind: String
  public let message: String
  public let extensions: GraphqlExtensions?
}

public struct SyncInfo: Codable, Sendable, Equatable {
  public var state: SyncState = .synced
  public var actions: [String] = []
  public var dirtyFields: [String] = []
  public var error: OperationFailure?
  public var attempts: Int = 0
  public var since: Int64 = 0
  public init() {}
}

public enum UploadStatus: String, Codable, Sendable { case waiting, preparing, queued, uploading, sending, done, failed, cancelled }

public struct UploadInfo: Codable, Sendable, Equatable {
  public let status: UploadStatus
  public let stage: String
  public let sentBytes: UInt64
  public let totalBytes: UInt64
  public let waitReason: String?
  /// Sum of retries for unfinished steps, preserved across restarts.
  public let attempts: Int
  public let error: OperationFailure?

  public var percent: Double { totalBytes == 0 ? 0 : min(100, Double(sentBytes) * 100 / Double(totalBytes)) }

  enum CodingKeys: String, CodingKey {
    case status, stage, attempts, error
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
    self.init()
    state = SyncState(wire: wire.state)
    actions = wire.actions
    dirtyFields = wire.dirtyFields
    error = wire.error.map(OperationFailure.init(wire:))
    attempts = Int(wire.attempts)
    since = wire.since
  }
}

extension UploadInfo {
  public init(wire: WireUpload) {
    self.init(
      status: UploadStatus(wire: wire.status), stage: wire.stage, sentBytes: wire.sentBytes, totalBytes: wire.totalBytes,
      waitReason: wire.waitReason, attempts: Int(wire.attempts), error: wire.error.map(OperationFailure.init(wire:))
    )
  }
}
