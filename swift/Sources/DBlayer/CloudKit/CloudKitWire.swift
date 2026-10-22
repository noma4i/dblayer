import CloudKit
import Foundation

struct CloudKitFetch {
  var records: [CKRecord]
  var token: Data
  var more: Bool
}

protocol CloudKitWire: Sendable {
  func accountStatus() async throws -> CKAccountStatus
  func prepare() async throws
  func fetch(token: Data?, limit: Int) async throws -> CloudKitFetch
  func save(_ records: [CKRecord]) async throws
}

private final class FetchState: @unchecked Sendable {
  var records: [CKRecord] = []
  var token: CKServerChangeToken?
  var more = false
  var failure: Error?
}

private final class SaveState: @unchecked Sendable {
  var failures: [CKRecord.ID: Error] = [:]
}

final class CloudKitContainerWire: CloudKitWire, @unchecked Sendable {
  private let container: CKContainer
  private let database: CKDatabase
  private let zoneID = CKRecordZone.ID(zoneName: CloudKitRecords.zoneName, ownerName: CKCurrentUserDefaultName)
  private let lock = NSLock()
  private var prepared = false

  init(container: CKContainer) {
    self.container = container
    database = container.privateCloudDatabase
  }

  var recordZoneID: CKRecordZone.ID { zoneID }

  func accountStatus() async throws -> CKAccountStatus {
    try await container.accountStatus()
  }

  func prepare() async throws {
    if lock.withLock({ prepared }) { return }
    try await saveZone()
    try await saveSubscription()
    lock.withLock { prepared = true }
  }

  func fetch(token: Data?, limit: Int) async throws -> CloudKitFetch {
    let options = CKFetchRecordZoneChangesOperation.ZoneConfiguration()
    options.previousServerChangeToken = try token.map(Self.decode)
    options.resultsLimit = limit
    let operation = CKFetchRecordZoneChangesOperation(recordZoneIDs: [zoneID], configurationsByRecordZoneID: [zoneID: options])
    operation.qualityOfService = .utility
    let state = FetchState()
    operation.recordWasChangedBlock = { _, result in
      switch result {
      case .success(let record): state.records.append(record)
      case .failure(let error): state.failure = state.failure ?? error
      }
    }
    operation.recordZoneFetchResultBlock = { _, result in
      switch result {
      case .success(let zone):
        state.token = zone.serverChangeToken
        state.more = zone.moreComing
      case .failure(let error):
        state.failure = state.failure ?? error
      }
    }
    try await run { done in
      operation.fetchRecordZoneChangesResultBlock = done
      self.database.add(operation)
    }
    if let failure = state.failure { throw failure }
    guard let newToken = state.token else { throw CKError(.internalError) }
    return CloudKitFetch(records: state.records, token: try Self.encode(newToken), more: state.more)
  }

  func save(_ records: [CKRecord]) async throws {
    let operation = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: nil)
    operation.savePolicy = .allKeys
    operation.isAtomic = false
    operation.qualityOfService = .utility
    let state = SaveState()
    operation.perRecordSaveBlock = { id, result in
      if case .failure(let error) = result { state.failures[id] = error }
    }
    try await run { done in
      operation.modifyRecordsResultBlock = done
      self.database.add(operation)
    }
    if !state.failures.isEmpty {
      throw CKError(.partialFailure, userInfo: [CKPartialErrorsByItemIDKey: state.failures])
    }
  }

  private func saveZone() async throws {
    let operation = CKModifyRecordZonesOperation(recordZonesToSave: [CKRecordZone(zoneID: zoneID)], recordZoneIDsToDelete: nil)
    operation.qualityOfService = .utility
    try await run { done in
      operation.modifyRecordZonesResultBlock = done
      self.database.add(operation)
    }
  }

  private func saveSubscription() async throws {
    let subscription = CKDatabaseSubscription(subscriptionID: CloudKitRecords.subscriptionID)
    let info = CKSubscription.NotificationInfo()
    info.shouldSendContentAvailable = true
    subscription.notificationInfo = info
    let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: nil)
    operation.qualityOfService = .utility
    try await run { done in
      operation.modifySubscriptionsResultBlock = done
      self.database.add(operation)
    }
  }

  private func run(_ start: (@escaping (Result<Void, Error>) -> Void) -> Void) async throws {
    try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
      start { continuation.resume(with: $0) }
    }
  }

  private static func encode(_ token: CKServerChangeToken) throws -> Data {
    try NSKeyedArchiver.archivedData(withRootObject: token, requiringSecureCoding: true)
  }

  private static func decode(_ data: Data) throws -> CKServerChangeToken {
    guard let token = try? NSKeyedUnarchiver.unarchivedObject(ofClass: CKServerChangeToken.self, from: data) else {
      throw CKError(.changeTokenExpired)
    }
    return token
  }
}
