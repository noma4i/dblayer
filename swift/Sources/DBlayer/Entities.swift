import Foundation
import Observation

public enum EntityPhase: Sendable, Equatable { case opening, present, absent, failed(String), closed }

public struct EntitySnapshot<Model: DBModel>: Sendable, Equatable {
  public let value: Model?
  public let phase: EntityPhase
  public let read: EntityRead
  public let revision: Int64
  public let isStale: Bool
  public let settled: Bool
  static var opening: Self { Self(value: nil, phase: .opening, read: .notLoaded, revision: .min, isStale: false, settled: false) }
}

@MainActor @Observable
public final class Entity<Model: DBModel> {
  public private(set) var snapshot: EntitySnapshot<Model> = .opening
  public var id: String { fixedId ?? snapshot.value?.id ?? "" }
  public var value: Model? { snapshot.value }
  public var phase: EntityPhase { snapshot.phase }
  public var read: EntityRead { snapshot.read }
  public var isStale: Bool { snapshot.isStale }
  public var isClosed: Bool { snapshot.phase == .closed }
  var revision: Int64 { snapshot.revision }
  @ObservationIgnored private let fixedId: String?
  @ObservationIgnored fileprivate let lease: UInt64
  @ObservationIgnored private let registry: Entities<Model>
  @ObservationIgnored private let readers = EntityReaders<Model>()
  @ObservationIgnored private var waiters: [UUID: CheckedContinuation<Void, Never>] = [:]

  fileprivate init(id: String?, lease: UInt64, registry: Entities<Model>) {
    fixedId = id
    self.lease = lease
    self.registry = registry
  }

  fileprivate func publish(_ next: EntitySnapshot<Model>) {
    let previous = snapshot.value
    snapshot = next
    if previous != next.value && next.phase != .closed { readers.put(next.value) }
    if next.settled || next.phase == .closed {
      let ready = waiters
      waiters = [:]
      ready.values.forEach { $0.resume() }
    }
    if next.phase == .closed { readers.closeAll() }
  }

  public func waitForInitialLoad() async throws {
    if !snapshot.settled && snapshot.phase != .closed {
      let key = UUID()
      await withTaskCancellationHandler {
        await withCheckedContinuation { continuation in
          if Task.isCancelled || snapshot.settled || snapshot.phase == .closed { continuation.resume() }
          else { waiters[key] = continuation }
        }
      } onCancel: {
        Task { @MainActor [self] in waiters.removeValue(forKey: key)?.resume() }
      }
    }
    try Task.checkCancellation()
    if phase == .closed { throw DbError.Interrupted(detail: "entity closed") }
    if case let .failed(error) = read { throw error }
  }

  public func changes() -> LiveStream<Model?> {
    guard phase != .closed else { return .ended }
    let latest = Latest<Model?>(merge: nil)
    latest.put(value)
    let key = readers.add(latest)
    let attachment = Attachment { [readers, entity = self] in readers.remove(key); withExtendedLifetime(entity) {} }
    readers.bind(key, attachment)
    return LiveStream(latest: latest, attachment: attachment)
  }

  isolated deinit {
    if let fixedId { registry.released(id: fixedId, lease: lease) }
  }
}

final class EntityReaders<Model: DBModel>: @unchecked Sendable {
  private struct Reader { let latest: Latest<Model?>; weak var attachment: Attachment? }
  private let lock = NSLock()
  private var readers: [Int: Reader] = [:]
  private var next = 0
  func add(_ latest: Latest<Model?>) -> Int { lock.withLock { next += 1; readers[next] = Reader(latest: latest); return next } }
  func bind(_ key: Int, _ attachment: Attachment) { lock.withLock { readers[key]?.attachment = attachment } }
  func remove(_ key: Int) { lock.withLock { readers[key] = nil } }
  func put(_ value: Model?) { lock.withLock { readers.values.map(\.latest) }.forEach { $0.put(value) } }
  func closeAll() {
    let current = lock.withLock { () -> [Reader] in defer { readers = [:] }; return Array(readers.values) }
    current.forEach { $0.latest.close(); $0.attachment?.cancel() }
  }
}

@MainActor
public final class Entities<Model: DBModel> {
  public static var shared: Entities<Model> { EntityRegistries.registry(for: Model.self) }
  private struct Weak { weak var entity: Entity<Model>? }
  private var live: [String: Weak] = [:]
  private var single: Entity<Model>?
  private var next: UInt64 = 0
  private var pending: [String: EntityRequest] = [:]
  private var flushing = false
  private let core: CoreEntitySource<Model>

  init() {
    core = CoreEntitySource()
    core.deliver = { [self] lease, id, snapshot in
      DispatchQueue.main.async { MainActor.assumeIsolated {
        let target = id.isEmpty ? self.single : self.live[id]?.entity
        if let target, target.lease == lease { target.publish(snapshot) }
        if snapshot.phase == .closed { if id.isEmpty { self.single = nil } else { self.live[id] = nil } }
      } }
    }
  }

  var decodedRows: Int { core.decodedRows }
  var leaseFailures: Int { core.leaseFailures }
  var liveCount: Int { live.values.filter { $0.entity != nil }.count }

  public func entity(_ id: String, fetch: EntityFetch = .none) -> Entity<Model> {
    if let existing = live[id]?.entity { return existing }
    next += 1
    let made = Entity<Model>(id: id, lease: next, registry: self)
    live[id] = Weak(entity: made)
    pending[id] = EntityRequest(id: id, lease: next, fetch: fetch)
    if !flushing {
      flushing = true
      Task { @MainActor [weak self] in self?.flush() }
    }
    return made
  }

  public func entities(_ ids: [String]) -> [Entity<Model>] { ids.map { entity($0) } }

  public func singleton(_ query: Query<Model>) -> Entity<Model> {
    if let single { return single }
    next += 1
    let made = Entity<Model>(id: nil, lease: next, registry: self)
    single = made
    core.singleton(query, lease: next)
    return made
  }

  private func flush() {
    flushing = false
    let requests = Array(pending.values)
    pending = [:]
    if !requests.isEmpty { core.retain(requests) }
  }

  fileprivate func released(id: String, lease: UInt64) {
    if live[id]?.entity == nil { live[id] = nil }
    if pending[id]?.lease == lease { pending[id] = nil }
    core.release([EntityLease(id: id, lease: lease)])
  }

  public func reset() { core.reset() }
  fileprivate func delivered() async { await core.delivered() }
  func close() {
    pending = [:]
    core.close()
    let held = live.values.compactMap(\.entity) + [single].compactMap { $0 }
    live = [:]
    single = nil
    for entity in held {
      entity.publish(EntitySnapshot(value: nil, phase: .closed, read: entity.snapshot.read, revision: entity.snapshot.revision, isStale: false, settled: true))
    }
  }
  fileprivate func resume() { core.resume() }
}

@MainActor
enum EntityRegistries {
  private static var registries: [ObjectIdentifier: AnyObject] = [:]
  private static var settles: [() async -> Void] = []
  private static var resumes: [() -> Void] = []
  private static var closes: [() -> Void] = []
  private static var failures: [() -> Int] = []
  static func registry<Model: DBModel>(for _: Model.Type) -> Entities<Model> {
    let key = ObjectIdentifier(Model.self)
    if let found = registries[key] as? Entities<Model> { return found }
    let made = Entities<Model>()
    registries[key] = made
    settles.append { await made.delivered() }
    resumes.append { made.resume() }
    closes.append { made.close() }
    failures.append { made.leaseFailures }
    return made
  }
  static func leaseFailures() -> Int { failures.reduce(0) { $0 + $1() } }
  static func settleAll() async { for settle in settles { await settle() } }
  static func closeAll() { closes.forEach { $0() }; registries = [:]; settles = []; resumes = []; closes = []; failures = [] }
  static func resumeAll() { resumes.forEach { $0() } }
}

extension DBModel {
  @MainActor public static func entity(_ id: String) -> Entity<Self> { Entities<Self>.shared.entity(id) }
  @MainActor public static func entities(_ ids: [String]) -> [Entity<Self>] { Entities<Self>.shared.entities(ids) }
}

public protocol DBSingleton: DBModel { static var singleton: Query<Self> { get } }
extension DBSingleton {
  @MainActor public static func entity() -> Entity<Self> { Entities<Self>.shared.singleton(singleton) }
}

final class CoreEntitySource<Model: DBModel>: @unchecked Sendable {
  private final class Changes: EntityListener, @unchecked Sendable {
    weak var source: CoreEntitySource?
    init(_ source: CoreEntitySource) { self.source = source }
    func changed(delivery: EntityDelivery) { source?.changed(delivery) }
  }
  private let queue = DispatchQueue(label: "dblayer.entities", qos: .userInitiated)
  private let lock = NSLock()
  private var observer: LiveEntities?
  private var total = 0
  private var values: [UInt64: (UInt64, Model?)] = [:]
  private var failures = 0
  var deliver: (@Sendable (UInt64, String, EntitySnapshot<Model>) -> Void)?
  var decodedRows: Int { lock.withLock { total } }
  var leaseFailures: Int { lock.withLock { failures } }

  private func open() throws -> LiveEntities {
    if let observer { return observer }
    let made = try DB.database.liveEntities(model: Model.modelName, listener: Changes(self))
    observer = made
    return made
  }

  func retain(_ requests: [EntityRequest]) {
    queue.async { self.perform(requests.map { EntityLease(id: $0.id, lease: $0.lease) }) { try self.open().retain(requests: requests) } }
  }
  func release(_ leases: [EntityLease]) {
    queue.async { leases.forEach { self.values[$0.lease] = nil }; guard let observer = self.observer else { return }; self.perform(leases) { try observer.release(leases: leases) } }
  }
  func singleton(_ query: Query<Model>, lease: UInt64) {
    queue.async { self.perform([EntityLease(id: "", lease: lease)]) { try self.open().singleton(queryJson: query.json, lease: lease) } }
  }
  func reset() { queue.async { self.observer?.reset() } }
  func resume() { queue.async { self.observer?.resume() } }
  func close() { queue.async { self.observer?.shutdown(); self.observer = nil; self.values = [:]; self.queue.async { self.deliver = nil } } }
  func delivered() async {
    await withCheckedContinuation { continuation in queue.async { DispatchQueue.main.async { continuation.resume() } } }
  }

  private func perform(_ leases: [EntityLease], _ call: () throws -> Void) {
    do { try call() } catch {
      lock.withLock { failures += 1 }
      for lease in leases { decode(entityOpenFailed(id: lease.id, lease: lease.lease, message: String(describing: error), unopened: error is DatabaseNotOpen)) }
    }
  }

  private func changed(_ delivery: EntityDelivery) {
    queue.async { delivery.snapshots.forEach { self.decode($0) } }
  }

  private func decode(_ wire: WireEntitySnapshot) {
    let value: Model?
    do {
      if let held = values[wire.lease], held.0 == wire.valueToken { value = held.1 }
      else { value = try wire.value.map { try Model.decode($0) }; values[wire.lease] = (wire.valueToken, value); if value != nil { lock.withLock { total += 1 } } }
    } catch {
      lock.withLock { failures += 1 }
      observer?.decodeFailed(lease: EntityLease(id: wire.id, lease: wire.lease), revision: wire.revision, message: String(describing: error))
      return
    }
    let phase: EntityPhase
    switch wire.phase {
    case .opening: phase = .opening
    case .present: phase = .present
    case .absent: phase = .absent
    case let .failed(message): phase = .failed(message)
    case .closed: phase = .closed
    }
    deliver?(wire.lease, wire.id, EntitySnapshot(value: value, phase: phase, read: wire.read, revision: wire.revision, isStale: wire.isStale, settled: wire.settled))
  }

  deinit {
    let held = observer
    queue.async { held?.shutdown(); withExtendedLifetime(held) {} }
  }
}

extension EntityRefreshError: Error {}
