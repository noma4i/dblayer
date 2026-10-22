import CloudKit
import Foundation
@testable import DBlayer

final class FakeCloudKitStore: @unchecked Sendable {
  private let lock = NSLock()
  private var log: [(seq: Int, record: CKRecord)] = []
  private(set) var saves = 0

  func save(_ records: [CKRecord]) {
    lock.withLock {
      saves += 1
      for record in records {
        log.removeAll { $0.record.recordID == record.recordID }
        log.append((log.count == 0 ? 1 : (log.map(\.seq).max() ?? 0) + 1, stored(record)))
      }
    }
  }

  private func stored(_ record: CKRecord) -> CKRecord {
    let copy = record.copy() as! CKRecord
    if let url = (record[CloudKitRecords.assetField] as? CKAsset)?.fileURL {
      let kept = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
      try? FileManager.default.copyItem(at: url, to: kept)
      copy[CloudKitRecords.assetField] = CKAsset(fileURL: kept)
    }
    return copy
  }

  func fetch(after seq: Int, limit: Int) -> (records: [CKRecord], seq: Int, more: Bool) {
    lock.withLock {
      let pending = log.filter { $0.seq > seq }.sorted { $0.seq < $1.seq }
      let page = Array(pending.prefix(limit))
      return (page.map(\.record), page.last?.seq ?? seq, pending.count > limit)
    }
  }
}

final class FakeCloudKitWire: CloudKitWire, @unchecked Sendable {
  let store: FakeCloudKitStore
  var status: CKAccountStatus = .available
  var failure: Error?
  private let lock = NSLock()
  private var tokens: [Data?] = []
  private var expired = false

  init(store: FakeCloudKitStore = FakeCloudKitStore()) { self.store = store }

  var fetchedTokens: [Data?] { lock.withLock { tokens } }

  func expireNextToken() { lock.withLock { expired = true } }

  func accountStatus() async throws -> CKAccountStatus {
    if let failure { throw failure }
    return status
  }

  func prepare() async throws {
    if let failure { throw failure }
  }

  func fetch(token: Data?, limit: Int) async throws -> CloudKitFetch {
    let expire = lock.withLock { () -> Bool in
      tokens.append(token)
      defer { expired = false }
      return expired && token != nil
    }
    if expire { throw CKError(.changeTokenExpired) }
    let after = token.flatMap { Int(String(decoding: $0, as: UTF8.self)) } ?? 0
    let page = store.fetch(after: after, limit: limit)
    return CloudKitFetch(records: page.records, token: Data(String(page.seq).utf8), more: page.more)
  }

  func save(_ records: [CKRecord]) async throws {
    if let failure { throw failure }
    store.save(records)
  }
}
