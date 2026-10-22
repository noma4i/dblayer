import CloudKit
import Foundation

enum CloudKitErrors {
  static func map(_ error: Error) -> CloudError {
    if let cloud = error as? CloudError { return cloud }
    if let kit = error as? CKError { return map(kit) }
    if error is URLError { return .Offline(detail: error.localizedDescription) }
    return .Unavailable(detail: String(describing: error))
  }

  static func map(_ error: CKError) -> CloudError {
    let detail = error.localizedDescription
    switch error.code {
    case .networkUnavailable, .networkFailure:
      return .Offline(detail: detail)
    case .requestRateLimited, .zoneBusy:
      return .Throttled(retryAfterMs: retryAfterMs(error))
    case .serviceUnavailable:
      return error.retryAfterSeconds == nil ? .Unavailable(detail: detail) : .Throttled(retryAfterMs: retryAfterMs(error))
    case .internalError, .serverResponseLost, .accountTemporarilyUnavailable:
      return .Unavailable(detail: detail)
    case .notAuthenticated, .permissionFailure, .managedAccountRestricted:
      return .NeedsAuthorization(detail: detail)
    case .quotaExceeded:
      return .QuotaExceeded(detail: detail)
    case .partialFailure:
      let inner = (error.partialErrorsByItemID ?? [:]).values.map { map($0) }
      return inner.max { rank($0) < rank($1) } ?? .Rejected(detail: detail)
    default:
      return .Rejected(detail: detail)
    }
  }

  private static func retryAfterMs(_ error: CKError) -> UInt64 {
    UInt64(((error.retryAfterSeconds ?? 0) * 1000).rounded(.up))
  }

  private static func rank(_ error: CloudError) -> Int {
    switch error {
    case .Offline: return 0
    case .Unavailable: return 1
    case .Throttled: return 2
    case .Rejected: return 3
    case .QuotaExceeded: return 4
    case .NeedsAuthorization: return 5
    }
  }
}
