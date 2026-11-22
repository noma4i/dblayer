import CloudKit
import CryptoKit
import Foundation

extension CloudConfig {
  public static func cloudKit(application: CloudApplication, container: CKContainer = .default()) -> CloudConfig {
    CloudConfig(backend: .cloudKit, application: application, port: CloudKitSync(container: container))
  }
}

actor CloudKitSync: CloudPort {
  private let container: CKContainer
  private var observation: UInt64 = 0
  private var accountRequest: UInt64 = 0
  private var observer: NSObjectProtocol?
  private var signals: CloudSignals?
  private var active: CloudKitGeneration?
  private var accountTask: Task<Void, Never>?

  init(container: CKContainer = .default()) { self.container = container }

  func observe(signals: CloudSignals) async {
    observation &+= 1
    accountRequest &+= 1
    let token = observation
    self.signals = signals
    let previous = observer
    observer = NotificationCenter.default.addObserver(forName: .CKAccountChanged, object: nil, queue: nil) { [weak self] _ in
      Task { await self?.resolveAccount(token: token) }
    }
    if let previous { NotificationCenter.default.removeObserver(previous) }
    resolveAccount(token: token)
  }

  func unobserve() async {
    observation &+= 1
    signals = nil
    let previous = observer
    observer = nil
    if let previous { NotificationCenter.default.removeObserver(previous) }
    let task = accountTask
    accountTask = nil
    task?.cancel()
    await task?.value
  }

  private func resolveAccount(token: UInt64) {
    guard observation == token, signals != nil else { return }
    accountRequest &+= 1
    let request = accountRequest
    accountTask?.cancel()
    accountTask = Task {
      let account: CloudAccount
      do {
        switch try await container.accountStatus() {
        case .available:
          account = .available(key: try await container.userRecordID().recordName)
        case .noAccount:
          account = .signedOut
        default:
          account = .unavailable
        }
      } catch {
        account = .unavailable
      }
      guard !Task.isCancelled, observation == token, accountRequest == request else { return }
      signals?.account(account: account)
    }
  }

  func start(scope: CloudWireScope, progress: Data?, signals: CloudSignals) async throws {
    let generation = try CloudKitGeneration(container: container, scope: scope, progress: progress, signals: signals) { [weak self] in
      Task { await self?.reportUnavailable(scope: scope) }
    }
    guard active == nil else { throw DbError.InFlight(detail: "cloud generation active") }
    active = generation
    do { try await generation.start() } catch {
      await stop(generation: scope.generation)
      throw error
    }
  }

  func stop(generation: UInt64) async {
    let current = active?.scope.generation == generation ? active : nil
    if current != nil { active = nil }
    await current?.stop()
  }

  func send(scope: CloudWireScope, records: [CloudOutbound]) async throws -> [CloudSendResult] {
    let current = active?.scope == scope ? active : nil
    guard let current else { throw DbError.NoSession(detail: "cloud generation ended") }
    return try await current.send(records)
  }

  private func reportUnavailable(scope: CloudWireScope) {
    let target = active?.scope == scope ? signals : nil
    target?.account(account: .unavailable)
  }
}

private actor CloudKitGeneration: CKSyncEngineDelegate {
  nonisolated let scope: CloudWireScope
  private let zone: CKRecordZone.ID
  private let signals: CloudSignals
  private let container: CKContainer
  private let savedState: CKSyncEngine.State.Serialization?
  private let unavailable: @Sendable () -> Void
  private var engine: CKSyncEngine?
  private var stopped = false
  private var fetchFailed = false
  private var landingFailed = false
  private var initial = true
  private var pendingState: Data?
  private var eventTail: Task<Void, Never>?
  private var outgoing: [CKRecord.ID: (CloudOutbound, CKRecord)] = [:]
  private var results: [String: CloudSendStatus] = [:]

  init(container: CKContainer, scope: CloudWireScope, progress: Data?, signals: CloudSignals, unavailable: @escaping @Sendable () -> Void) throws {
    self.container = container
    self.scope = scope
    self.signals = signals
    self.unavailable = unavailable
    zone = CKRecordZone.ID(zoneName: scope.domain, ownerName: CKCurrentUserDefaultName)
    savedState = try progress.map { try JSONDecoder().decode(CKSyncEngine.State.Serialization.self, from: $0) }
  }

  func start() async throws {
    guard !stopped else { throw DbError.NoSession(detail: "cloud generation ended") }
    try await verifyAccount()
    if savedState == nil {
      _ = try await container.privateCloudDatabase.save(CKRecordZone(zoneID: zone))
      guard !stopped else { throw DbError.NoSession(detail: "cloud generation ended") }
    }
    var config = CKSyncEngine.Configuration(database: container.privateCloudDatabase, stateSerialization: savedState, delegate: self)
    config.automaticallySync = true
    let engine = CKSyncEngine(config)
    self.engine = engine
    try await engine.fetchChanges(.init(scope: .zoneIDs([zone])))
    guard !stopped, !fetchFailed, !landingFailed else { throw DbError.Runtime(detail: "cloud fetch failed") }
  }

  func stop() async {
    stopped = true
    let current = engine
    engine = nil
    outgoing.removeAll()
    await current?.cancelOperations()
  }

  func send(_ records: [CloudOutbound]) async throws -> [CloudSendResult] {
    guard !stopped, !landingFailed, let engine else { throw DbError.NoSession(detail: "cloud generation ended") }
    try await verifyAccount()
    var immediate: [CloudSendResult] = []
    for item in records {
      do {
        let record = try CloudKitRecord.make(zone: zone, key: item.key, payload: item.payload, systemFields: item.systemFields)
        outgoing[record.recordID] = (item, record)
      } catch {
        immediate.append(CloudSendResult(key: item.key, version: item.version, status: .failed(detail: "invalid cloud record")))
      }
    }
    results.removeAll()
    let changes = outgoing.keys.map { CKSyncEngine.PendingRecordZoneChange.saveRecord($0) }
    engine.state.add(pendingRecordZoneChanges: changes)
    do { try await engine.sendChanges(.init(scope: .zoneIDs([zone]))) } catch {
      guard !stopped else { throw DbError.NoSession(detail: "cloud generation ended") }
    }
    guard !stopped else { throw DbError.NoSession(detail: "cloud generation ended") }
    let sent = outgoing.values.map { item, _ in
      CloudSendResult(key: item.key, version: item.version, status: results[item.key] ?? .retryable(detail: "cloud send incomplete"))
    }
    engine.state.remove(pendingRecordZoneChanges: changes)
    outgoing.removeAll()
    results.removeAll()
    return immediate + sent
  }

  func nextFetchChangesOptions(_ context: CKSyncEngine.FetchChangesContext, syncEngine: CKSyncEngine) async -> CKSyncEngine.FetchChangesOptions {
    .init(scope: .zoneIDs([zone]))
  }

  func nextRecordZoneChangeBatch(_ context: CKSyncEngine.SendChangesContext, syncEngine: CKSyncEngine) async -> CKSyncEngine.RecordZoneChangeBatch? {
    guard !stopped, !landingFailed, engine === syncEngine else { return nil }
    let records = outgoing.values.map { $0.1 }.filter { context.options.scope.contains($0.recordID) }
    return records.isEmpty ? nil : CKSyncEngine.RecordZoneChangeBatch(recordsToSave: records)
  }

  func handleEvent(_ event: CKSyncEngine.Event, syncEngine: CKSyncEngine) async {
    if case .accountChange(let change) = event, engine === syncEngine {
      switch change.changeType {
      case .signOut:
        revoke(syncEngine)
      case .signIn(let currentUser), .switchAccounts(_, let currentUser):
        if Self.fingerprint(currentUser.recordName) != scope.providerFingerprint { revoke(syncEngine) }
      @unknown default:
        revoke(syncEngine)
      }
    }
    let previous = eventTail
    let next = Task {
      await previous?.value
      await self.apply(event, syncEngine: syncEngine)
    }
    eventTail = next
    await next.value
  }

  private static func fingerprint(_ key: String) -> String {
    SHA256.hash(data: Data(key.utf8)).map { String(format: "%02x", $0) }.joined()
  }

  private func verifyAccount() async throws {
    let recordName = try await container.userRecordID().recordName
    guard !stopped else { throw DbError.NoSession(detail: "cloud generation ended") }
    guard Self.fingerprint(recordName) == scope.providerFingerprint else {
      if let engine { revoke(engine) }
      throw DbError.NoSession(detail: "cloud account changed")
    }
  }

  private func revoke(_ syncEngine: CKSyncEngine) {
    stopped = true
    pendingState = nil
    Task { await syncEngine.cancelOperations() }
  }

  private func apply(_ event: CKSyncEngine.Event, syncEngine: CKSyncEngine) async {
    guard !stopped, !landingFailed, engine === syncEngine else { return }
    switch event {
    case .willFetchChanges:
      fetchFailed = false
    case .fetchedRecordZoneChanges(let changes):
      do {
        guard changes.deletions.isEmpty else { throw CloudKitRecord.Failure.invalidRecord }
        let records = try changes.modifications.map { change in
          let value = try CloudKitRecord.values(change.record, zone: zone)
          return CloudInbound(key: value.key, payload: value.payload, systemFields: value.systemFields)
        }
        try await signals.fetched(fetched: CloudFetched(scope: scope, records: records, progress: nil, initialComplete: false))
      } catch { refuseLanding(syncEngine) }
    case .didFetchRecordZoneChanges(let result):
      if result.zoneID == zone, result.error != nil { fetchFailed = true }
    case .stateUpdate(let state):
      do { pendingState = try JSONEncoder().encode(state.stateSerialization) } catch { fetchFailed = true }
    case .didFetchChanges:
      guard !fetchFailed, !stopped else { return }
      do {
        try await signals.fetched(fetched: CloudFetched(scope: scope, records: [], progress: pendingState, initialComplete: initial))
        guard !stopped else { return }
        initial = false
        pendingState = nil
      } catch { refuseLanding(syncEngine) }
    case .sentRecordZoneChanges(let changes):
      for record in changes.savedRecords {
        guard outgoing[record.recordID] != nil else { continue }
        do {
          let value = try CloudKitRecord.values(record, zone: zone)
          results[value.key] = .saved(systemFields: value.systemFields)
        } catch { results[record.recordID.recordName] = .retryable(detail: "invalid cloud record") }
      }
      for failure in changes.failedRecordSaves {
        let key = failure.record.recordID.recordName
        guard outgoing[failure.record.recordID] != nil else { continue }
        if failure.error.code == .serverRecordChanged, let record = failure.error.serverRecord {
          do {
            let value = try CloudKitRecord.values(record, zone: zone)
            results[key] = .conflict(record: CloudInbound(key: value.key, payload: value.payload, systemFields: value.systemFields))
          } catch {
            results[key] = .retryable(detail: "invalid cloud conflict")
          }
        } else {
          switch failure.error.code {
          case .networkUnavailable, .networkFailure, .serviceUnavailable, .requestRateLimited, .zoneBusy, .notAuthenticated:
            results[key] = .retryable(detail: "cloud send unavailable")
          default:
            results[key] = .failed(detail: "cloud send rejected")
          }
        }
      }
    default: break
    }
  }

  private func refuseLanding(_ syncEngine: CKSyncEngine) {
    landingFailed = true
    pendingState = nil
    Task {
      await syncEngine.cancelOperations()
      self.reportLandingFailure(syncEngine)
    }
  }

  private func reportLandingFailure(_ syncEngine: CKSyncEngine) {
    guard !stopped, engine === syncEngine else { return }
    unavailable()
  }
}
