import CloudKit
import Foundation
import Testing
@testable import DBlayer

private let zone = CKRecordZone.ID(zoneName: CloudKitRecords.zoneName, ownerName: CKCurrentUserDefaultName)

private func blob(_ key: String, size: Int = 8) -> CloudBlob {
  CloudBlob(key: key, bytes: Data((0..<size).map { UInt8(truncatingIfNeeded: $0 &* 7 &+ key.count) }))
}

private func adapter(_ wire: FakeCloudKitWire) -> CloudKitAdapter { CloudKitAdapter(wire: wire, zoneID: zone) }

struct CloudKitAdapterTests {
  @Test
  func accountStatusesMapToCloudAccounts() async throws {
    let table: [(CKAccountStatus, CloudAccount)] = [(.available, .available), (.noAccount, .signedOut), (.restricted, .restricted), (.temporarilyUnavailable, .needsConsent)]
    for (status, expected) in table {
      let wire = FakeCloudKitWire()
      wire.status = status
      #expect(try await adapter(wire).account() == expected)
    }
    let unknown = FakeCloudKitWire()
    unknown.status = .couldNotDetermine
    await #expect(throws: CloudError.self) { try await adapter(unknown).account() }
  }

  @Test
  func aPushWithoutAnAccountNeedsAuthorization() async {
    let wire = FakeCloudKitWire()
    wire.failure = CKError(.notAuthenticated)
    do {
      try await adapter(wire).push(blobs: [blob("a")])
      Issue.record("expected a failure")
    } catch let error as CloudError {
      if case .NeedsAuthorization = error { return }
      Issue.record("got \(error)")
    } catch {
      Issue.record("got \(error)")
    }
  }

  @Test
  func blobsComeBackWithTheSameKeysAndBytes() async throws {
    let wire = FakeCloudKitWire()
    let sent = [blob("a"), blob("b", size: 0), blob("c", size: CloudKitRecords.inlineLimit + 5)]
    try await adapter(wire).push(blobs: sent)
    let page = try await adapter(wire).pull(cursor: nil, limit: 10)
    #expect(page.blobs == sent)
  }

  @Test
  func pullsPageByTokenAndAnEmptyCursorStartsOver() async throws {
    let wire = FakeCloudKitWire()
    let io = adapter(wire)
    try await io.push(blobs: [blob("a"), blob("b")])
    let first = try await io.pull(cursor: Data(), limit: 1)
    #expect(first.blobs.count == 1 && first.more)
    let second = try await io.pull(cursor: first.cursor, limit: 1)
    #expect(second.blobs.count == 1 && !second.more)
    #expect(wire.fetchedTokens == [nil, first.cursor])
  }

  @Test
  func anExpiredTokenRefetchesFromTheStart() async throws {
    let wire = FakeCloudKitWire()
    let io = adapter(wire)
    try await io.push(blobs: [blob("a")])
    let first = try await io.pull(cursor: nil, limit: 10)
    wire.expireNextToken()
    let again = try await io.pull(cursor: first.cursor, limit: 10)
    #expect(again.blobs == first.blobs)
    #expect(wire.fetchedTokens == [nil, first.cursor, nil])
  }

  @Test
  func errorsFromTheWireAreMapped() async {
    let wire = FakeCloudKitWire()
    wire.failure = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: NSNumber(value: 4)])
    await #expect(throws: CloudError.Throttled(retryAfterMs: 4000)) { try await adapter(wire).pull(cursor: nil, limit: 10) }
  }
}
