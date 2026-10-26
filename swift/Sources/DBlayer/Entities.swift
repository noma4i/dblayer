import Foundation
import Observation
import OSLog

private let entityLog = Logger(subsystem: "DBlayer", category: "entities")

public enum EntityPhase: Sendable, Equatable { case opening, present, absent, failed(String), closed }

@MainActor
@Observable
public final class Entity<Model: DBModel> {
  public private(set) var id: String
  public private(set) var value: Model?
  public private(set) var isStale: Bool
  public private(set) var phase: EntityPhase = .opening
  public private(set) var read: EntityRead = .notLoaded
  public var isClosed: Bool { phase == .closed }
  @ObservationIgnored private(set) var revision: Int64 = Int64.min
  @ObservationIgnored private var bytes: UInt64 = 0
  @ObservationIgnored private let registry: Entities<Model>
  @ObservationIgnored private let isSingleton: Bool
  @ObservationIgnored private let machine: EntityMachine
  @ObservationIgnored private let readers = EntityReaders<Model>()
  @ObservationIgnored private var settled = true
  @ObservationIgnored private var waiters: [Int: CheckedContinuation<Void, Never>] = [:]
  @ObservationIgnored private var nextWaiter = 0

  fileprivate init(id: String, registry: Entities<Model>) {
    self.id = id
    value = nil
    isStale = false
    self.registry = registry
    isSingleton = false
    machine = EntityMachine(singleton: false)
  }

  fileprivate init(singletonOf registry: Entities<Model>) {
    id = ""
    value = nil
    isStale = false
    self.registry = registry
    isSingleton = true
    machine = EntityMachine(singleton: true)
  }

  private func show(_ verdict: EntityVerdict) {
    revision = verdict.revision
    let next: EntityPhase
    switch verdict.stage {
    case .opening: next = .opening
    case .present: next = .present
    case .absent: next = .absent
    case let .failed(message): next = .failed(message)
    case .closed: next = .closed
    }
    if phase != next { phase = next }
    if isStale != verdict.stale { isStale = verdict.stale }
    if read != verdict.read { read = verdict.read }
    settled = verdict.settled
    if settled { wake() }
  }

  private func wake() {
    let ready = waiters
    waiters = [:]
    ready.values.forEach { $0.resume() }
  }

  /// Returns when the first read has ended or a value is already there, throws the error of a failed read, and sends no request of its own: it waits for the read the entity started.
  public func waitForInitialLoad() async throws {
    if !settled {
      let key = nextWaiter
      nextWaiter += 1
      await withTaskCancellationHandler {
        await withCheckedContinuation { continuation in
          if Task.isCancelled || settled { continuation.resume() } else { waiters[key] = continuation }
        }
      } onCancel: {
        Task { @MainActor [self] in waiters.removeValue(forKey: key)?.resume() }
      }
    }
    try Task.checkCancellation()
    if phase == .closed { throw DbError.Interrupted(detail: "entity closed") }
    if case let .failed(error) = read { throw error }
  }

  fileprivate func seed(revision incoming: Int64, bytes: UInt64, _ make: () throws -> Model?) {
    let verdict = machine.seed(revision: incoming)
    guard verdict.applied else { return }
    do { value = try make() } catch { fail(String(describing: error)); return }
    self.bytes = bytes
    show(verdict)
  }

  fileprivate func accept(_ next: Model?, revision incoming: Int64, bytes: UInt64 = 0, feed: UInt64? = nil) {
    let verdict = machine.accept(revision: incoming, present: next != nil, feed: feed)
    guard verdict.applied else { return }
    self.bytes = bytes
    show(verdict)
    if next != value { value = next; readers.put(next) }
    if isSingleton, id != (next?.id ?? "") { id = next?.id ?? "" }
  }

  fileprivate func readStarted() {
    show(machine.readStarted())
  }

  fileprivate func readEnded(_ error: EntityRefreshError?, feed: UInt64? = nil) {
    show(machine.readEnded(error: error, feed: feed))
  }

  fileprivate func fail(_ message: String, feed: UInt64? = nil) {
    show(machine.fail(message: message, feed: feed))
  }

  fileprivate func close() {
    emptied(machine.databaseClosed())
    readers.closeAll()
  }

  fileprivate func unopened(feed: UInt64) -> Bool {
    let verdict = machine.unopened(feed: feed)
    guard verdict.cleared else { return false }
    emptied(verdict)
    readers.closeAll()
    return true
  }

  fileprivate func reset() {
    let verdict = machine.reset()
    guard verdict.cleared else { return }
    if value != nil { readers.put(nil) }
    emptied(verdict)
  }

  fileprivate func beginFeed() -> UInt64 {
    let verdict = machine.beginFeed()
    show(verdict)
    return verdict.feed
  }

  private func emptied(_ verdict: EntityVerdict) {
    value = nil
    bytes = 0
    if isSingleton, !id.isEmpty { id = "" }
    show(verdict)
  }

  /// The value now, then each newer one. A reader that falls behind gets the newest value only. The stream keeps this entity alive until it ends, is cancelled or is dropped, and ends when the database closes. Read `phase` for the reason a value is missing.
  public func changes() -> LiveStream<Model?> {
    guard phase != .closed else { return .ended }
    let latest = Latest<Model?>(merge: nil)
    latest.put(value)
    let key = readers.add(latest)
    let attachment = Attachment { [readers, entity = self] in
      readers.remove(key)
      withExtendedLifetime(entity) {}
    }
    readers.bind(key, attachment)
    return LiveStream(latest: latest, attachment: attachment)
  }

  isolated deinit {
    guard !isSingleton, phase != .closed else { return }
    registry.released(id: id, value: value, absent: phase == .absent, revision: revision, bytes: bytes)
  }
}

final class EntityReaders<Model: DBModel>: @unchecked Sendable {
  private struct Reader {
    let latest: Latest<Model?>
    weak var attachment: Attachment?
  }

  private let lock = NSLock()
  private var readers: [Int: Reader] = [:]
  private var next = 0

  func add(_ latest: Latest<Model?>) -> Int {
    lock.withLock { next += 1; readers[next] = Reader(latest: latest); return next }
  }

  func bind(_ key: Int, _ attachment: Attachment) { lock.withLock { readers[key]?.attachment = attachment } }

  func remove(_ key: Int) { lock.withLock { readers[key] = nil } }

  func put(_ value: Model?) {
    let current = lock.withLock { readers.values.map(\.latest) }
    current.forEach { $0.put(value) }
  }

  func closeAll() {
    let current = lock.withLock { () -> [Reader] in defer { readers = [:] }; return Array(readers.values) }
    current.forEach { $0.latest.close(); $0.attachment?.cancel() }
  }
}

struct Retained<Model: DBModel> {
  let value: Model?
  let revision: Int64
  let bytes: UInt64
}

protocol EntitySource: AnyObject, Sendable {
  func cachedRow(_ id: String) throws -> WireRow?
  func retain(_ ids: [String], fetch: EntityFetch)
  func release(_ ids: [String])
}

@MainActor
public final class Entities<Model: DBModel> {
  public static var shared: Entities<Model> { EntityRegistries.registry(for: Model.self) }

  private enum Step { case retain, release, refresh }

  private struct Weak { weak var entity: Entity<Model>? }

  private var live: [String: Weak] = [:]
  private let kept: RetainedValues
  private var steps: [String: Step] = [:]
  private var fetches: [String: EntityFetch] = [:]
  private var flushScheduled = false
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
    core.deliver = { [weak self] delivery in
      DispatchQueue.main.async { MainActor.assumeIsolated { self?.apply(delivery) } }
    }
  }

  var decodedRows: Int { core.decodedRows }
  var peekedRows: Int { core.peekedRows }
  var leaseFailures: Int { core.leaseFailures }

  public func entity(_ id: String, fetch: EntityFetch = .none) -> Entity<Model> {
    if let existing = live[id]?.entity { return existing }
    let retained = kept.take(id, as: Model.self, now: UInt64(now() * 1000))
    let made = Entity<Model>(id: id, registry: self)
    if let retained {
      if let value = retained.value { made.seed(revision: retained.revision, bytes: retained.bytes) { value } } else { made.accept(nil, revision: retained.revision, bytes: retained.bytes) }
    }
    if retained == nil {
      do {
        if let cached = try source.cachedRow(id) { made.seed(revision: cached.revision, bytes: cached.estimatedBytes) { try Model.decode(cached) } }
      } catch { made.fail(String(describing: error)) }
    }
    if fetch != .none { made.readStarted() }
    live[id] = Weak(entity: made)
    steps[id] = (retained != nil || steps[id] == .release) ? .refresh : .retain
    fetches[id] = fetch
    scheduleFlush()
    return made
  }

  public func entities(_ ids: [String]) -> [Entity<Model>] { ids.map { entity($0) } }

  public func singleton(_ query: Query<Model>) -> Entity<Model> {
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
    guard let single else { return }
    let feed = SingletonFeed<Model>(query: singleQuery, generation: single.beginFeed())
    singleFeed = feed
    feed.start(deliver: { [weak self, weak feed] revision, row in
      DispatchQueue.main.async { MainActor.assumeIsolated { if let self, let feed { self.receive(from: feed, revision: revision, row: row) } } }
    }, unopened: { [weak self, weak feed] in
      DispatchQueue.main.async { MainActor.assumeIsolated { if let self, let feed { self.unopened(feed) } } }
    }, read: { [weak self, weak feed] error in
      DispatchQueue.main.async { MainActor.assumeIsolated { if let self, let feed { self.readEnded(from: feed, error: error) } } }
    }, failed: { [weak self, weak feed] message in
      DispatchQueue.main.async { MainActor.assumeIsolated { if let self, let feed { self.single?.fail(message, feed: feed.generation) } } }
    })
  }

  func readEnded(from feed: SingletonFeed<Model>, error: EntityRefreshError?) {
    single?.readEnded(error, feed: feed.generation)
  }

  func receive(from feed: SingletonFeed<Model>, revision: Int64, row: Model?) {
    single?.accept(row, revision: revision, feed: feed.generation)
  }

  fileprivate func unopened(_ feed: SingletonFeed<Model>) {
    guard single?.unopened(feed: feed.generation) == true else { return }
    singleFeed = nil
    single = nil
    singleQuery = nil
  }

  public func reset() {
    kept.clear(Model.modelName)
    singleFeed?.cancel()
    singleFeed = nil
    single?.reset()
  }

  func close() {
    live.values.forEach { $0.entity?.close() }
    live = [:]
    steps = [:]
    fetches = [:]
    kept.clear(Model.modelName)
    singleFeed?.cancel()
    singleFeed = nil
    single?.close()
    single = nil
    singleQuery = nil
    core.close()
  }

  fileprivate func resume() {
    if singleFeed == nil { startSingleton() }
  }

  var retainedCount: Int {
    kept.count(Model.modelName, now: UInt64(now() * 1000))
  }

  var liveCount: Int { live.values.filter { $0.entity != nil }.count }

  fileprivate func released(id: String, value: Model?, absent: Bool, revision: Int64, bytes: UInt64) {
    guard live[id]?.entity == nil else { return }
    live[id] = nil
    steps[id] = .release
    if value != nil || absent { kept.keep(id, value: Retained(value: value, revision: revision, bytes: bytes), now: UInt64(now() * 1000)) }
    scheduleFlush()
  }

  func apply(_ delivery: EntityDecoding.Decoded<Model>) {
    for (model, bytes) in delivery.rows { live[model.id]?.entity?.accept(model, revision: delivery.revision, bytes: bytes) }
    for id in delivery.removed { live[id]?.entity?.accept(nil, revision: delivery.revision) }
    if let message = delivery.failure { live.values.forEach { $0.entity?.fail(message) } }
    for refresh in delivery.refreshes { live[refresh.id]?.entity?.readEnded(refresh.error) }
    for (id, message) in delivery.failedIds { live[id]?.entity?.fail(message) }
  }

  private func scheduleFlush() {
    guard !flushScheduled else { return }
    flushScheduled = true
    Task { @MainActor [weak self] in self?.flush() }
  }

  private func flush() {
    flushScheduled = false
    let pending = steps
    let wanted = fetches
    steps = [:]
    fetches = [:]
    let release = pending.filter { $0.value != .retain }.map(\.key)
    let retain = Dictionary(grouping: pending.filter { $0.value != .release }.map(\.key)) { wanted[$0] ?? .none }
    if !release.isEmpty { source.release(release) }
    for (fetch, ids) in retain { source.retain(ids, fetch: fetch) }
  }
}

@MainActor
enum EntityRegistries {
  static let retained = RetainedValues()
  private static var registries: [ObjectIdentifier: AnyObject] = [:]
  private static var resets: [() -> Void] = []
  private static var resumes: [() -> Void] = []
  private static var closes: [() -> Void] = []
  private static var failures: [() -> Int] = []

  static func registry<Model: DBModel>(for _: Model.Type) -> Entities<Model> {
    let key = ObjectIdentifier(Model.self)
    if let found = registries[key] as? Entities<Model> { return found }
    let made = Entities<Model>()
    registries[key] = made
    resets.append { made.reset() }
    resumes.append { made.resume() }
    closes.append { made.close() }
    failures.append { made.leaseFailures }
    return made
  }

  static func leaseFailures() -> Int { failures.reduce(0) { $0 + $1() } }
  static func resetAll() { resets.forEach { $0() } }
  static func closeAll() { closes.forEach { $0() } }
  static func resumeAll() { resumes.forEach { $0() } }
}

extension DBModel {
  @MainActor public static func entity(_ id: String) -> Entity<Self> { Entities<Self>.shared.entity(id) }
  @MainActor public static func entities(_ ids: [String]) -> [Entity<Self>] { Entities<Self>.shared.entities(ids) }
}

public protocol DBSingleton: DBModel {
  static var singleton: Query<Self> { get }
}

extension DBSingleton {
  @MainActor public static func entity() -> Entity<Self> { Entities<Self>.shared.singleton(singleton) }
}

enum EntityDecoding {
  struct Decoded<Model: DBModel> {
    let revision: Int64
    let rows: [(Model, UInt64)]
    let removed: [String]
    var failure: String?
    var failedIds: [String: String] = [:]
    var refreshes: [EntityRefresh] = []
  }
}

final class CoreEntitySource<Model: DBModel>: EntitySource, @unchecked Sendable {
  private final class Changes: EntityListener, @unchecked Sendable {
    weak var source: CoreEntitySource?
    init(_ source: CoreEntitySource) { self.source = source }
    func changed(delivery: EntityDelivery) { source?.changed(delivery: delivery) }
  }

  private let queue = DispatchQueue(label: "dblayer.entities", qos: .userInitiated)
  private var observer: LiveEntities?
  private let lock = NSLock()
  private var total = 0
  private var failures = 0
  private var peeks = 0
  var deliver: (@Sendable (EntityDecoding.Decoded<Model>) -> Void)?

  var decodedRows: Int { lock.withLock { total } }
  var peekedRows: Int { lock.withLock { peeks } }
  var leaseFailures: Int { lock.withLock { failures } }

  private func refused(_ ids: [String], _ call: () throws -> Void) {
    do { try call() } catch {
      lock.withLock { failures += 1 }
      entityLog.error("Entity lease of \(Model.modelName, privacy: .public) failed: \(String(describing: error), privacy: .public)")
      deliver?(EntityDecoding.Decoded(revision: Int64.min, rows: [], removed: [], failedIds: Dictionary(ids.map { ($0, String(describing: error))}, uniquingKeysWith: { first, _ in first })))
    }
  }

  func cachedRow(_ id: String) throws -> WireRow? {
    lock.withLock { peeks += 1 }
    return try DB.database.peekRow(model: Model.modelName, rowId: id)
  }

  func retain(_ ids: [String], fetch: EntityFetch) {
    queue.async { self.refused(ids) { try self.open().retain(ids: ids, fetch: fetch) } }
  }

  func release(_ ids: [String]) {
    queue.async { self.refused(ids) { try self.open().release(ids: ids) } }
  }

  func close() {
    queue.async { self.observer = nil }
  }

  private func open() throws -> LiveEntities {
    if let observer { return observer }
    let made = try DB.database.liveEntities(model: Model.modelName, listener: Changes(self))
    observer = made
    return made
  }

  private func changed(delivery: EntityDelivery) {
    if let failure = delivery.failure {
      lock.withLock { failures += 1 }
      entityLog.error("Entity read of \(Model.modelName, privacy: .public) failed: \(failure, privacy: .public)")
    }
    var failedIds: [String: String] = [:]
    let rows = delivery.rows.compactMap { row -> (Model, UInt64)? in
      do { return (try Model.decode(row), row.estimatedBytes) } catch {
        lock.withLock { failures += 1 }
        entityLog.error("Row of \(Model.modelName, privacy: .public) failed to decode for its entity: \(String(describing: error), privacy: .public)")
        failedIds[row.id] = String(describing: error)
        return nil
      }
    }
    lock.withLock { total += rows.count }
    deliver?(EntityDecoding.Decoded(revision: delivery.revision, rows: rows, removed: delivery.removed, failure: delivery.failure, failedIds: failedIds, refreshes: delivery.refreshes))
  }
}

final class SingletonFeed<Model: DBModel>: @unchecked Sendable {
  private let lock = NSLock()
  private let query: Query<Model>
  let generation: UInt64
  private var session: ListSession<Model, (Int64, Model?)>?
  private var cancelled = false
  private var count = 0

  var delivered: Int { lock.withLock { count } }

  init(query: Query<Model>, generation: UInt64) {
    self.query = query
    self.generation = generation
  }

  func start(deliver: @escaping @Sendable (Int64, Model?) -> Void, unopened: @escaping @Sendable () -> Void, read: @escaping @Sendable (EntityRefreshError?) -> Void, failed: @escaping @Sendable (String) -> Void) {
    Task.detached(priority: .userInitiated) { [self] in
      let made = ListSession<Model, (Int64, Model?)>(
        open: { [query] listener in ListHandle(try DB.database.liveQuery(queryJson: query.json, listener: listener)) },
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
      if keep {
        do { try made.start() } catch is DatabaseNotOpen { unopened(); return } catch { failed(String(describing: error)); return }
        do { try await made.waitForInitialLoad(); read(nil) } catch is CancellationError {} catch { read(.failed(message: String(describing: error))) }
      }
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

extension EntityRefreshError: Error {}
