import Foundation

public enum TransportRetryReason: String, Equatable, Sendable {
  case persistedQueryNotFound
  case signatureExpired
  case signatureReplay
}

public struct TransportRetryPolicy: Sendable {
  private var usedReasons: Set<TransportRetryReason> = []

  public init() {}

  public mutating func shouldRetry(reason: TransportRetryReason, kind: GraphQLOperationKind) -> Bool {
    if reason == .signatureReplay, kind != .query {
      return false
    }
    if usedReasons.contains(reason) {
      return false
    }
    usedReasons.insert(reason)
    return true
  }

  public static func reason(forErrorMessage message: String, code: String?) -> TransportRetryReason? {
    if message == "PersistedQueryNotFound" || code == "PERSISTED_QUERY_NOT_FOUND" {
      return .persistedQueryNotFound
    }
    if code == "SIGNATURE_EXPIRED" {
      return .signatureExpired
    }
    if code == "SIGNATURE_REPLAY" {
      return .signatureReplay
    }
    return nil
  }
}
