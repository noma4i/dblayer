import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct TransportRetryTests {
  @Test
  func retriesOncePerIndependentReasonAndReplayOnlyForQueries() {
    var policy = TransportRetryPolicy()
    let firstPersistedQuery = policy.shouldRetry(reason: .persistedQueryNotFound, kind: .query)
    let firstSignatureExpired = policy.shouldRetry(reason: .signatureExpired, kind: .query)
    let secondPersistedQuery = policy.shouldRetry(reason: .persistedQueryNotFound, kind: .query)
    let secondSignatureExpired = policy.shouldRetry(reason: .signatureExpired, kind: .query)
    #expect(firstPersistedQuery && firstSignatureExpired)
    #expect(!secondPersistedQuery && !secondSignatureExpired)

    var queryPolicy = TransportRetryPolicy()
    let firstReplay = queryPolicy.shouldRetry(reason: .signatureReplay, kind: .query)
    let secondReplay = queryPolicy.shouldRetry(reason: .signatureReplay, kind: .query)
    #expect(firstReplay)
    #expect(!secondReplay)

    var mutationPolicy = TransportRetryPolicy()
    let mutationReplay = mutationPolicy.shouldRetry(reason: .signatureReplay, kind: .mutation)
    #expect(!mutationReplay)
  }

  @Test
  func recognisesServerErrors() {
    #expect(
      TransportRetryPolicy.reason(forErrorMessage: "PersistedQueryNotFound", code: nil)
        == .persistedQueryNotFound
    )
    #expect(
      TransportRetryPolicy.reason(forErrorMessage: "Boom", code: "PERSISTED_QUERY_NOT_FOUND")
        == .persistedQueryNotFound
    )
    #expect(
      TransportRetryPolicy.reason(forErrorMessage: "Boom", code: "SIGNATURE_EXPIRED")
        == .signatureExpired
    )
    #expect(
      TransportRetryPolicy.reason(forErrorMessage: "Boom", code: "SIGNATURE_REPLAY")
        == .signatureReplay
    )
    #expect(TransportRetryPolicy.reason(forErrorMessage: "Boom", code: nil) == nil)
  }
}
