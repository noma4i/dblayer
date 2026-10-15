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
  public private(set) var isStale: Bool
  @ObservationIgnored private(set) var revision: Int64
  @ObservationIgnored private var bytes: UInt64 = 0
  @ObservationIgnored private let registry: Entities<Model>
  @ObservationIgnored private let epoch: Int
  @ObservationIgnored private let isSingleton: Bool

  fileprivate init(id: String, seed: Retained<Model>?, registry: Entities<Model>) {
    self.id = id
    value = seed?.value
    revision = seed?.revision ?? Int64.min
    bytes = seed?.bytes ?? 0
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

  fileprivate func accept(_ next: Model?, revision incoming: Int64, bytes: UInt64 = 0) {
    guard incoming >= revision else { return }
    revision = incoming
    self.bytes = bytes
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
    registry.released(id: id, value: value, revision: revision, bytes: bytes, epoch: epoch)
  }
}

struct Retained<Model: DBModel> {
  let value: Model?
  let revision: Int64
  let bytes: UInt64
}

protocol EntitySource: AnyObject, Sendable {
  func cachedRow(_ id: String) -> WireRow?
  func retain(_ ids: [String])
  func release(_ ids: [String])
}

/// The registry of one model: one `Entity` per id, one core observer for the whole model.
@MainActor
public final class Entities<Model: DBModel> {
  public static var shared: Entities<Model> { EntityRegistries.registry(for: Model.self) }

  private enum Step { case retain, release, refresh }

  private struct Weak { weak var entity: Entity<Model>? }

  fileprivate private(set) var epoch = 0
  private var live: [String: Weak] = [:]
  private let kept: RetainedValues
  private var steps: [String: Step] = [:]
  private var flushScheduled = false
  private var database: Database?
  private let followsDatabase: Bool
  private let source: any EntitySource
  private let now: @Sendable () -> TimeInterval

  private let core: CoreEntitySource<Model>
  private var single: Entity<Model>?
  private var singleQuery: Query<Model>?
  private(set) var singleFeed: SingletonFeed<Model>?

  init(now: @escaping @Sendable () -> TimeInterval = { ProcessInfo.processInfo.systemUptime }, source: (any EntitySource)? = nil) {
    kept = source == nil ? EntityRegistries.retained : RetainedValues()
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
    let retained = kept.take(id, as: Model.self, now: UInt64(now() * 1000))
    var seed = retained
    if let cached = source.cachedRow(id), cached.revision > (seed?.revision ?? Int64.min), let value = try? Model.decode(cached) {
      seed = Retained(value: value, revision: cached.revision, bytes: cached.estimatedBytes)
    }
    let made = Entity<Model>(id: id, seed: seed, registry: self)
    live[id] = Weak(entity: made)
    steps[id] = (retained != nil || steps[id] == .release) ? .refresh : .retain
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
    let all = Array(live.keys)
    live.values.forEach { $0.entity?.detach() }
    live = [:]
    kept.clear(Model.modelName)
    for id in all { steps[id] = .release }
    scheduleFlush()
    if let single {
      single.detach()
      startSingleton()
    }
  }

  var retainedCount: Int {
    kept.count(Model.modelName, now: UInt64(now() * 1000))
  }

  var liveCount: Int { live.values.filter { $0.entity != nil }.count }

  fileprivate func released(id: String, value: Model?, revision: Int64, bytes: UInt64, epoch entityEpoch: Int) {
    guard entityEpoch == epoch, live[id]?.entity == nil else { return }
    live[id] = nil
    steps[id] = .release
    if let value { kept.keep(id, value: Retained(value: value, revision: revision, bytes: bytes), now: UInt64(now() * 1000)) }
    scheduleFlush()
  }

  func apply(_ delivery: EntityDecoding.Decoded<Model>) {
    for (model, bytes) in delivery.rows { live[model.id]?.entity?.accept(model, revision: delivery.revision, bytes: bytes) }
    for id in delivery.removed { live[id]?.entity?.accept(nil, revision: delivery.revision) }
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
  static let retained = RetainedValues()
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
    let rows: [(Model, UInt64)]
    let removed: [String]
  }
}

/// Opens the one core observer of a model on first use and keeps its calls off the main thread, in order.
final class CoreEntitySource<Model: DBModel>: EntitySource, @unchecked Sendable {
  private final class Changes: EntityListener, @unchecked Sendable {
    weak var source: CoreEntitySource?
    init(_ source: CoreEntitySource) { self.source = source }
    func changed(delivery: EntityDelivery) { source?.changed(delivery: delivery) }
  }

  private let queue = DispatchQueue(label: "dblayer.entities", qos: .userInitiated)
  private var observer: LiveEntities?
  private var database: Database?
  private var leased = Set<String>()
  private let lock = NSLock()
  private var total = 0
  var deliver: (@Sendable (EntityDecoding.Decoded<Model>) -> Void)?

  var decodedRows: Int { lock.withLock { total } }

  func cachedRow(_ id: String) -> WireRow? {
    try? DB.database?.peekRow(model: Model.modelName, rowId: id)
  }

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
      observer = try? current.liveEntities(model: Model.modelName, listener: Changes(self))
      if !leased.isEmpty { try? observer?.retain(ids: Array(leased)) }
    }
    return observer
  }

  func changed(delivery: EntityDelivery) {
    let rows = delivery.rows.compactMap { row in (try? Model.decode(row)).map { ($0, row.estimatedBytes) } }
    lock.withLock { total += rows.count }
    deliver?(EntityDecoding.Decoded(revision: delivery.revision, rows: rows, removed: delivery.removed))
  }
}

/// The one relation subscription of a singleton. It belongs to one database and one reset: a replaced feed writes nothing.
final class SingletonFeed<Model: DBModel>: @unchecked Sendable {
  private let lock = NSLock()
  private let queryJson: String
  private var session: ListSession<Model, (Int64, Model?)>?
  private var cancelled = false
  private var count = 0

  /// Deliveries made so far.
  var delivered: Int { lock.withLock { count } }

  init(query: Query<Model>) { queryJson = query.json }

  func start(deliver: @escaping @Sendable (Int64, Model?) -> Void) {
    Task.detached(priority: .userInitiated) { [self] in
      guard let database = DB.database else { return }
      let made = ListSession<Model, (Int64, Model?)>(
        open: { [queryJson] listener in let subscription = try database.liveQuery(queryJson: queryJson, listener: listener); return { subscription.cancel() } },
        emit: { value in
          self.lock.withLock { self.count += 1 }
          deliver(value.0, value.1)
        },
        map: { applied, change in (change.revision, applied.rows.first) }
      )
      let keep = lock.withLock { () -> Bool in
        guard !cancelled else { return false }
        session = made
        return true
      }
      if keep { try? made.start() }
    }
  }

  func cancel() {
    let current = lock.withLock { () -> ListSession<Model, (Int64, Model?)>? in
      cancelled = true
      defer { session = nil }
      return session
    }
    current?.cancel()
  }
}
