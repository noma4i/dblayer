import Foundation
import Observation
import Testing
import DBlayerTesting
@testable import DBlayer

enum WaitFailure: Error { case timeout, unexpectedOutcome }

func entered(_ hold: Hold) async -> Bool {
  await withTaskGroup(of: Bool.self) { group in
    group.addTask { await hold.entered(); return !Task.isCancelled }
    group.addTask { try? await Task.sleep(for: .seconds(10)); return false }
    let arrived = await group.next() ?? false
    group.cancelAll()
    return arrived
  }
}

func valueBeforeDeadline<Value: Sendable>(_ task: Task<Value, any Error>, release: @escaping @Sendable () -> Void) async throws -> Value {
  try await withThrowingTaskGroup(of: Value.self) { group in
    group.addTask { try await task.value }
    group.addTask {
      try await Task.sleep(for: .seconds(10))
      task.cancel()
      release()
      throw WaitFailure.timeout
    }
    defer { group.cancelAll() }
    return try await group.next()!
  }
}
let hangGuard: Duration = .seconds(60)
func eventually(
  _ what: String = "the condition",
  timeout: Duration = hangGuard,
  isolation: isolated (any Actor)? = #isolation,
  sourceLocation: SourceLocation = #_sourceLocation,
  _ condition: () async -> Bool
) async {
  let deadline = ContinuousClock.now + timeout
  while ContinuousClock.now < deadline {
    if await condition() {
      return
    }
    try? await Task.sleep(for: .milliseconds(5))
  }
  if await condition() {
    return
  }
  Issue.record("timed out after \(timeout) waiting for \(what)", sourceLocation: sourceLocation)
}
final class Done: @unchecked Sendable {
  private let lock = NSLock()
  private var flag = false
  var value: Bool { lock.withLock { flag } }
  func set() { lock.withLock { flag = true } }
}
/// Runs `body` on its own task and records an issue when it does not return within the guard, so a call that hangs fails the test instead of stopping it.
@discardableResult
func finishes(
  _ what: String,
  timeout: Duration = hangGuard,
  isolation: isolated (any Actor)? = #isolation,
  sourceLocation: SourceLocation = #_sourceLocation,
  _ body: @escaping @Sendable () async -> Void
) async -> Bool {
  let done = Done()
  Task {
    await body()
    done.set()
  }
  await eventually("\(what) to return", timeout: timeout, isolation: isolation, sourceLocation: sourceLocation) { done.value }
  return done.value
}
func parkUntilCancelled() async throws {
  try await Task.sleep(for: .seconds(3600))
}
final class Cursor<Sequence: AsyncSequence & SendableMetatype>: @unchecked Sendable where Sequence.Element: Sendable, Sequence.AsyncIterator: SendableMetatype {
  private var iterator: Sequence.AsyncIterator

  init(_ sequence: Sequence) {
    iterator = sequence.makeAsyncIterator()
  }

  func next(
    _ what: String = "the next element",
    timeout: Duration = hangGuard,
    sourceLocation: SourceLocation = #_sourceLocation
  ) async -> Sequence.Element? {
    let element: Sequence.Element?? = await withTaskGroup(of: Sequence.Element??.self) { group in
      group.addTask { [self] in .some(try? await iterator.next()) }
      group.addTask {
        try? await Task.sleep(for: timeout)
        return nil
      }
      let first = await group.next() ?? nil
      group.cancelAll()
      return first
    }
    guard let element else {
      Issue.record("timed out after \(timeout) waiting for \(what)", sourceLocation: sourceLocation)
      return nil
    }
    return element
  }
  func first(
    _ what: String,
    timeout: Duration = hangGuard,
    sourceLocation: SourceLocation = #_sourceLocation,
    where predicate: @Sendable (Sequence.Element) -> Bool
  ) async -> Sequence.Element? {
    let deadline = ContinuousClock.now + timeout
    while ContinuousClock.now < deadline {
      let remaining = deadline - ContinuousClock.now
      guard let element = await next(what, timeout: remaining, sourceLocation: sourceLocation) else {
        return nil
      }
      if predicate(element) {
        return element
      }
    }
    Issue.record("timed out after \(timeout) waiting for \(what)", sourceLocation: sourceLocation)
    return nil
  }
  func collect(_ count: Int, _ what: String, timeout: Duration = hangGuard, sourceLocation: SourceLocation = #_sourceLocation) async -> [Sequence.Element] {
    let deadline = ContinuousClock.now + timeout
    var elements: [Sequence.Element] = []
    while elements.count < count, ContinuousClock.now < deadline {
      guard let element = await next(what, timeout: deadline - ContinuousClock.now, sourceLocation: sourceLocation) else {
        break
      }
      elements.append(element)
    }
    return elements
  }
}

func carriesRows(_ delivery: StreamDelivery) -> Bool {
  switch delivery.value {
  case .snapshot, .delta: return true
  default: return false
  }
}

@MainActor
func observe(_ what: String, _ condition: @escaping @MainActor () -> Bool) async -> Bool {
  let (changes, continuation) = AsyncStream<Void>.makeStream()
  defer { continuation.finish() }
  let cursor = Cursor(changes)
  while true {
    if withObservationTracking({ condition() }, onChange: { continuation.yield(()) }) { return true }
    guard await cursor.next(what, timeout: .seconds(20)) != nil else { return false }
  }
}

extension Query {
  func current() async throws -> [Model] { try await read() }
}

struct RowRead<Model: DBModel>: Sendable {
  let id: String
  func current() async throws -> Model? { try await Query<Model>().readRow(id) }
}

extension DBModel {
  static func find(_ id: String) -> RowRead<Self> { RowRead(id: id) }
}

struct TestSnapshot<Model: DBModel>: Sendable {
  let rows: [Model]
  let phase: LivePhase?
  let error: DbError?
  let total: Int?
  let hasMore: Bool
}

struct TestLive<Model: DBModel>: Sendable {
  let query: Query<Model>

  func current() async throws -> [Model] { try await query.current() }

  func stream() async throws -> AsyncStream<[Model]> {
    let value = try AccountScope().stream(query)
    return AsyncStream { continuation in
      let task = Task {
        var projection = ListProjection<Model>()
        for await delivery in value {
          let applied = projection.apply(delivery)
          if applied == .resync { value.resync() }
          if applied == .changed, carriesRows(delivery) { continuation.yield(projection.rows) }
        }
        continuation.finish()
      }
      continuation.onTermination = { _ in task.cancel(); value.cancel() }
    }
  }
}

struct TestSnapshots<Model: DBModel>: Sendable {
  let query: Query<Model>

  func stream() async throws -> AsyncStream<TestSnapshot<Model>> {
    let value = try AccountScope().stream(query)
    return AsyncStream { continuation in
      let task = Task {
        var projection = ListProjection<Model>()
        for await delivery in value {
          let applied = projection.apply(delivery)
          if applied == .resync { value.resync() }
          if applied == .changed { continuation.yield(TestSnapshot(rows: projection.rows, phase: projection.phase, error: projection.state?.error, total: projection.state?.total.map { Int($0) }, hasMore: projection.state?.hasMore ?? false)) }
        }
        continuation.finish()
      }
      continuation.onTermination = { _ in task.cancel(); value.cancel() }
    }
  }
}

extension Query {
  var live: TestLive<Model> { TestLive(query: self) }
  var snapshots: TestSnapshots<Model> { TestSnapshots(query: self) }
}

struct TestCount<Model: DBModel>: Sendable {
  let query: Query<Model>
  func current() async throws -> Int { try await query.current().count }
}

extension Query {
  var count: TestCount<Model> { TestCount(query: self) }
}

struct TestListState<Model: DBModel>: Sendable {
  let phase: LivePhase?
  let total: Int?
  let hasMore: Bool
  let loadedAt: UInt64?
  let errorKind: String?
  let complete: Bool
  let error: DbError?
  let rows: [Model]
}

@MainActor
final class ListReader<Model: DBModel> {
  let projected: Projected<Model>

  init(_ query: Query<Model>, refreshOnConnect: Bool = false) {
    projected = Projected(try! AccountScope().stream(query, refreshOnConnect: refreshOnConnect))
  }

  var rows: [Model] { projected.rows }
  var ids: [String] { projected.rows.map(\.id) }
  var snapshot: TestListState<Model> {
    let state = projected.state
    return TestListState(phase: state?.phase, total: state?.total.map { Int($0) }, hasMore: state?.hasMore ?? false, loadedAt: state?.loadedAt.map { UInt64($0) }, errorKind: state?.errorKind, complete: state?.complete ?? false, error: state?.error, rows: projected.rows)
  }
  func refresh() async throws { try await projected.refresh() }
  func waitForInitialLoad() async throws { try await projected.loaded() }
  func loadMore() async throws -> LoadMoreResult { try await projected.loadMore() }
}

extension MutationReceipt {
  func confirmed() async throws -> Model {
    guard case let .confirmed(value?, _) = try await outcome() else { throw WaitFailure.unexpectedOutcome }
    return value
  }

  func answer<Answer: Decodable>(as _: Answer.Type) async throws -> Answer {
    guard case let .confirmed(_, answerJSON?) = try await outcome() else { throw WaitFailure.unexpectedOutcome }
    return try JSONDecoder().decode(Answer.self, from: Data(answerJSON.utf8))
  }

  func refusal() async throws -> WireFailure {
    guard case let .rejected(failure) = try await outcome() else { throw WaitFailure.unexpectedOutcome }
    return failure
  }
}

/// Reads the journal of a stream the way `ValueStream` does and hands each value to `take`. A test makes it the sink of `subscribe`, then binds the stream number it got.
final class PulledStream: ValueSink, @unchecked Sendable {
  private let lock = NSLock()
  private let reading = NSLock()
  private var bound: (stream: UInt64, database: Database)?
  private let take: @Sendable (StreamDelivery) -> Void

  init(_ take: @escaping @Sendable (StreamDelivery) -> Void) { self.take = take }

  func bind(_ stream: UInt64, _ database: Database) {
    lock.withLock { bound = (stream, database) }
    available()
  }

  func available() {
    guard let bound = lock.withLock({ bound }) else { return }
    reading.withLock {
      while let delivery = bound.database.readStream(stream: bound.stream) { take(delivery) }
    }
  }
}

func stateOf(_ delivery: StreamDelivery) -> StreamState? {
  switch delivery.value {
  case let .snapshot(_, _, state), let .count(_, _, state), let .state(state): return state
  case let .delta(_, _, _, _, _, _, _, _, _, state): return state
  default: return nil
  }
}

/// A reader of the events of a model once its source is open: the first item it read was `.ready`.
func opened<Model: DBModel>(_ events: EventStream<Model>, sourceLocation: SourceLocation = #_sourceLocation) async -> Cursor<EventStream<Model>> {
  let cursor = Cursor(events)
  guard case .ready? = await cursor.next("the source of the events opens", sourceLocation: sourceLocation) else {
    Issue.record("the first item of the events stream was not ready", sourceLocation: sourceLocation)
    return cursor
  }
  return cursor
}
