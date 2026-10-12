import Foundation
import Observation

/// The live value of one record. The library keeps it current while the object is alive and lets go of the record when the last holder drops it.
@MainActor
@Observable
public final class Entity<Model: DBModel> {
  /// The id of the record. A singleton has no id until its row has loaded.
  public private(set) var id: String
  /// The newest value, nil while none has arrived or when the record does not exist.
  public private(set) var value: Model?
  /// The value comes from the retention of a released entity and no fresh delivery has replaced it yet.
  public private(set) var isStale: Bool
  @ObservationIgnored private(set) var revision: Int64
  @ObservationIgnored private let registry: Entities<Model>
  @ObservationIgnored private let epoch: Int
  @ObservationIgnored private let isSingleton: Bool

  fileprivate init(id: String, seed: Retained<Model>?, registry: Entities<Model>) {
    self.id = id
    value = seed?.value
    revision = seed?.revision ?? Int64.min
    isStale = seed != nil
    self.registry = registry
    epoch = registry.epoch
    isSingleton = false
  }

  fileprivate init(singletonOf registry: Entities<Model>) {
    id = ""
    value = nil
    revision = Int64.min
    isStale = false
    self.registry = registry
    epoch = registry.epoch
    isSingleton = true
  }

  fileprivate func accept(_ next: Model?, revision incoming: Int64) {
    guard incoming >= revision else { return }
    revision = incoming
    isStale = false
    if next != value { value = next }
    if isSingleton, id != (next?.id ?? "") { id = next?.id ?? "" }
  }

  fileprivate func detach() {
    value = nil
    guard isSingleton else { return }
    id = ""
    revision = Int64.min
    isStale = false
  }

  isolated deinit {
    guard !isSingleton else { return }
    registry.released(id: id, value: value, revision: revision, epoch: epoch)
  }
}

struct Retained<Model: DBModel> {
  let value: Model?
  let revision: Int64
  let releasedAt: TimeInterval
}

protocol EntitySource: AnyObject, Sendable {
  func retain(_ ids: [String])
  func release(_ ids: [String])
}

/// The registry of one model: one `Entity` per id, one core observer for the whole model.
@MainActor
public final class Entities<Model: DBModel> {
  public static var shared: Entities<Model> { EntityRegistries.registry(for: Model.self) }

  nonisolated static var retentionAge: TimeInterval { 120 }
  nonisolated static var retentionCount: Int { 200 }

  private enum Step { case retain, release, refresh }

  private struct Weak { weak var entity: Entity<Model>? }

  fileprivate private(set) var epoch = 0
  private var live: [String: Weak] = [:]
  private var kept: [String: Retained<Model>] = [:]
  private var steps: [String: Step] = [:]
  private var flushScheduled = false
  private var database: Database?
  private let followsDatabase: Bool
  private let source: any EntitySource
  private let now: @Sendable () -> TimeInterval
  private let maxAge: TimeInterval
  private let maxCount: Int

  private let core: CoreEntitySource<Model>
  private var single: Entity<Model>?
  private var singleQuery: Query<Model>?
  private(set) var singleFeed: SingletonFeed<Model>?

  init(maxAge: TimeInterval = Entities.retentionAge, maxCount: Int = Entities.retentionCount, now: @escaping @Sendable () -> TimeInterval = { ProcessInfo.processInfo.systemUptime }, source: (any EntitySource)? = nil) {
    self.maxAge = maxAge
    self.maxCount = maxCount
    self.now = now
    let core = CoreEntitySource<Model>()
    self.core = core
    self.source = source ?? core
    followsDatabase = source == nil
    core.deliver = { [weak self] delivery in
      DispatchQueue.main.async { MainActor.assumeIsolated { self?.apply(delivery) } }
    }
  }

  /// Rows decoded from deliveries so far: 1 per changed row.
  var decodedRows: Int { core.decodedRows }

  /// The entity of an id. Hold it for as long as the value is shown. The value is there at once when the id is live or retained.
  public func entity(_ id: String) -> Entity<Model> {
    follow(DB.database)
    if let existing = live[id]?.entity { return existing }
    purge()
    let seed = kept.removeValue(forKey: id)
    let made = Entity<Model>(id: id, seed: seed, registry: self)
    live[id] = Weak(entity: made)
    steps[id] = (seed != nil || steps[id] == .release) ? .refresh : .retain
    scheduleFlush()
    return made
  }

  /// A newly opened database starts with no entity of the old one.
  private func follow(_ current: Database?) {
    guard followsDatabase, database !== current else { return }
    if database != nil || single != nil { reset() }
    database = current
  }

  public func entities(_ ids: [String]) -> [Entity<Model>] { ids.map(entity) }

  /// The entity of the one row of a model that has one. It lives as long as the database handle, holds no lease and has no id until the row has loaded.
  public func singleton(_ query: Query<Model>) -> Entity<Model> {
    follow(DB.database)
    if let single { return single }
    let made = Entity<Model>(singletonOf: self)
    single = made
    singleQuery = query
    startSingleton()
    return made
  }

  private func startSingleton() {
    guard let singleQuery else { return }
    singleFeed?.cancel()
    let feed = SingletonFeed<Model>(query: singleQuery)
    singleFeed = feed
    feed.start { [weak self, weak feed] revision, row in
      DispatchQueue.main.async { MainActor.assumeIsolated { if let self, let feed { self.receive(from: feed, revision: revision, row: row) } } }
    }
  }

  /// A delivery of a feed that was replaced by a reset or a newly opened database writes nothing.
  func receive(from feed: SingletonFeed<Model>, revision: Int64, row: Model?) {
    guard singleFeed === feed else { return }
    single?.accept(row, revision: revision)
  }

  /// Drops every entity, lease and retained value. Called when the account ends.
  public func reset() {
    epoch += 1
    let all = Array(live.keys) + Array(kept.keys)
    live.values.forEach { $0.entity?.detach() }
    live = [:]
    kept = [:]
    steps = Dictionary(uniqueKeysWithValues: all.map { ($0, Step.release) })
    scheduleFlush()
    if let single {
      single.detach()
      startSingleton()
    }
  }

  var retainedCount: Int {
    purge()
    return kept.count
  }

  var liveCount: Int { live.values.filter { $0.entity != nil }.count }

  fileprivate func released(id: String, value: Model?, revision: Int64, epoch entityEpoch: Int) {
    guard entityEpoch == epoch, live[id]?.entity == nil else { return }
    live[id] = nil
    steps[id] = .release
    if let value { keep(id, Retained(value: value, revision: revision, releasedAt: now())) }
    scheduleFlush()
  }

  func apply(_ delivery: EntityDecoding.Decoded<Model>) {
    for model in delivery.rows { live[model.id]?.entity?.accept(model, revision: delivery.revision) }
    for id in delivery.removed { live[id]?.entity?.accept(nil, revision: delivery.revision) }
  }

  private func keep(_ id: String, _ retained: Retained<Model>) {
    kept[id] = retained
    purge()
    if kept.count > maxCount, let oldest = kept.min(by: { $0.value.releasedAt < $1.value.releasedAt })?.key { kept[oldest] = nil }
  }

  private func purge() {
    let cutoff = now() - maxAge
    if kept.contains(where: { $0.value.releasedAt <= cutoff }) { kept = kept.filter { $0.value.releasedAt > cutoff } }
  }

  private func scheduleFlush() {
    guard !flushScheduled else { return }
    flushScheduled = true
    Task { @MainActor [weak self] in self?.flush() }
  }

  private func flush() {
    flushScheduled = false
    let pending = steps
    steps = [:]
    let release = pending.filter { $0.value != .retain }.map(\.key)
    let retain = pending.filter { $0.value != .release }.map(\.key)
    if !release.isEmpty { source.release(release) }
    if !retain.isEmpty { source.retain(retain) }
  }
}

@MainActor
enum EntityRegistries {
  private static var registries: [ObjectIdentifier: AnyObject] = [:]
  private static var resets: [() -> Void] = []

  static func registry<Model: DBModel>(for _: Model.Type) -> Entities<Model> {
    let key = ObjectIdentifier(Model.self)
    if let found = registries[key] as? Entities<Model> { return found }
    let made = Entities<Model>()
    registries[key] = made
    resets.append { made.reset() }
    return made
  }

  static func resetAll() { resets.forEach { $0() } }
}

extension DBModel {
  /// The live entity of a record by id.
  @MainActor public static func entity(_ id: String) -> Entity<Self> { Entities<Self>.shared.entity(id) }
  /// The live entities of several ids.
  @MainActor public static func entities(_ ids: [String]) -> [Entity<Self>] { Entities<Self>.shared.entities(ids) }
}

/// A model with one row for the whole account, such as the current user. The generator declares it for a model that has one global relation answered as a single record.
public protocol DBSingleton: DBModel {
  /// The relation that holds the one row.
  static var singleton: Query<Self> { get }
}

extension DBSingleton {
  /// The live entity of the one row. Empty until the row has loaded and again after `DB.logout()`. Hold it as long as the value is shown.
  @MainActor public static func entity() -> Entity<Self> { Entities<Self>.shared.singleton(singleton) }
}

enum EntityDecoding {
  struct Decoded<Model: DBModel> {
    let revision: Int64
    let rows: [Model]
    let removed: [String]
  }
}

/// Opens the one core observer of a model on first use and keeps its calls off the main thread, in order.
final class CoreEntitySource<Model: DBModel>: EntitySource, EntityListener, @unchecked Sendable {
  private let queue = DispatchQueue(label: "dblayer.entities", qos: .userInitiated)
  private var observer: LiveEntities?
  private var database: Database?
  private var leased = Set<String>()
  private let lock = NSLock()
  private var total = 0
  var deliver: (@Sendable (EntityDecoding.Decoded<Model>) -> Void)?

  var decodedRows: Int { lock.withLock { total } }

  func retain(_ ids: [String]) {
    queue.async {
      self.leased.formUnion(ids)
      try? self.open()?.retain(ids: ids)
    }
  }

  func release(_ ids: [String]) {
    queue.async {
      self.leased.subtract(ids)
      try? self.open()?.release(ids: ids)
    }
  }

  /// The observer belongs to the database that was current when it opened. A newly opened database gets a new observer with the ids still leased.
  private func open() -> LiveEntities? {
    guard let current = DB.database else { return nil }
    if observer == nil || database !== current {
      database = current
      observer = try? current.liveEntities(model: Model.modelName, listener: self)
      if !leased.isEmpty { try? observer?.retain(ids: Array(leased)) }
    }
    return observer
  }

  func changed(delivery: EntityDelivery) {
    let decoder = JSONDecoder()
    let rows = delivery.rows.compactMap { try? decoder.decode(Model.self, from: Data($0.utf8)) }
    lock.withLock { total += rows.count }
    deliver?(EntityDecoding.Decoded(revision: delivery.revision, rows: rows, removed: delivery.removed))
  }
}

/// The one relation subscription of a singleton. It belongs to one database and one reset: a replaced feed writes nothing.
final class SingletonFeed<Model: DBModel>: @unchecked Sendable {
  private let lock = NSLock()
  private let queryJson: String
  private var subscription: LiveSubscription?
  private var cancelled = false
  private var count = 0

  /// Deliveries made so far.
  var delivered: Int { lock.withLock { count } }

  init(query: Query<Model>) { queryJson = query.json }

  func start(deliver: @escaping @Sendable (Int64, Model?) -> Void) {
    Task.detached(priority: .userInitiated) { [self] in
      guard let database = DB.database else { return }
      let feed = ListFeed<Model>()
      let listener = ChangeListener { change in
        self.lock.withLock { self.count += 1 }
        deliver(change.revision, feed.apply(change).rows.first)
      }
      guard let made = try? database.liveQuery(queryJson: queryJson, listener: listener) else { return }
      let keep = lock.withLock { () -> Bool in
        guard !cancelled else { return false }
        subscription = made
        return true
      }
      if !keep { made.cancel() }
    }
  }

  func cancel() {
    let current = lock.withLock { () -> LiveSubscription? in
      cancelled = true
      defer { subscription = nil }
      return subscription
    }
    current?.cancel()
  }
}
