import Foundation

public enum SyncState: String, Codable, Sendable { case synced, pending, sending, failed, deliveryUnknown = "delivery_unknown" }

public struct OperationFailure: Codable, Sendable, Equatable { public let kind: String; public let message: String }

public struct SyncInfo: Codable, Sendable, Equatable {
  public var state: SyncState = .synced
  public var actions: [String] = []
  public var dirtyFields: [String] = []
  public var error: OperationFailure?
  public var attempts: Int = 0
  public var since: Int64 = 0
  public init() {}
}

public enum UploadStatus: String, Codable, Sendable { case waiting, preparing, queued, uploading, sending, done, failed, deliveryUnknown = "delivery_unknown", cancelled }

public struct UploadInfo: Codable, Sendable, Equatable {
  public let status: UploadStatus
  public let stage: String
  public let sentBytes: UInt64
  public let totalBytes: UInt64
  public let waitReason: String?
  public let attempts: Int
  public let error: OperationFailure?

  public var percent: Double { totalBytes == 0 ? 0 : min(100, Double(sentBytes) * 100 / Double(totalBytes)) }

  enum CodingKeys: String, CodingKey {
    case status, stage, attempts, error
    case sentBytes = "sent_bytes", totalBytes = "total_bytes", waitReason = "wait_reason"
  }
}
