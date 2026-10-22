import CloudKit
import Foundation
import Testing
@testable import DBlayer

private func retry(_ seconds: Double) -> [String: Any] { [CKErrorRetryAfterKey: NSNumber(value: seconds)] }

struct CloudKitErrorsTests {
  @Test
  func cloudKitErrorsMapToCloudErrors() {
    let partial = CKError(.partialFailure, userInfo: [CKPartialErrorsByItemIDKey: [
      "a": CKError(.networkFailure),
      "b": CKError(.quotaExceeded),
      "c": CKError(.requestRateLimited, userInfo: retry(3)),
    ] as [String: Error]])
    let table: [(Error, CloudError)] = [
      (CKError(.networkUnavailable), .Offline(detail: CKError(.networkUnavailable).localizedDescription)),
      (CKError(.networkFailure), .Offline(detail: CKError(.networkFailure).localizedDescription)),
      (CKError(.requestRateLimited, userInfo: retry(2.5)), .Throttled(retryAfterMs: 2500)),
      (CKError(.zoneBusy, userInfo: retry(30)), .Throttled(retryAfterMs: 30_000)),
      (CKError(.serviceUnavailable, userInfo: retry(1)), .Throttled(retryAfterMs: 1000)),
      (CKError(.serviceUnavailable), .Unavailable(detail: CKError(.serviceUnavailable).localizedDescription)),
      (CKError(.internalError), .Unavailable(detail: CKError(.internalError).localizedDescription)),
      (CKError(.notAuthenticated), .NeedsAuthorization(detail: CKError(.notAuthenticated).localizedDescription)),
      (CKError(.permissionFailure), .NeedsAuthorization(detail: CKError(.permissionFailure).localizedDescription)),
      (CKError(.quotaExceeded), .QuotaExceeded(detail: CKError(.quotaExceeded).localizedDescription)),
      (CKError(.badContainer), .Rejected(detail: CKError(.badContainer).localizedDescription)),
      (CKError(.limitExceeded), .Rejected(detail: CKError(.limitExceeded).localizedDescription)),
      (partial, .QuotaExceeded(detail: CKError(.quotaExceeded).localizedDescription)),
      (URLError(.notConnectedToInternet), .Offline(detail: URLError(.notConnectedToInternet).localizedDescription)),
      (CloudError.Rejected(detail: "kept"), .Rejected(detail: "kept")),
    ]
    for (error, expected) in table {
      #expect(CloudKitErrors.map(error) == expected, "\(error)")
    }
  }

  @Test
  func anUnknownErrorIsUnavailable() {
    struct Odd: Error {}
    guard case .Unavailable = CloudKitErrors.map(Odd()) else {
      Issue.record("expected unavailable")
      return
    }
  }
}
