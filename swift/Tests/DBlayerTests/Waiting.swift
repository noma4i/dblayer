import Foundation
import Network
import Observation
import Testing
import DBlayerTesting
@testable import DBlayer

enum WaitFailure: Error { case timeout, unexpectedOutcome }

/// Returns when `timeout` has passed on the wall clock, or at once when the task is cancelled. A limit raced against an event, never a pause.
func expiry(_ timeout: Duration) async throws {
  let (expired, fire) = AsyncStream<Void>.makeStream()
  let timer = DispatchSource.makeTimerSource(queue: .global())
  let parts = timeout.components
  timer.schedule(deadline: .now() + .nanoseconds(Int(parts.seconds) * 1_000_000_000 + Int(parts.attoseconds / 1_000_000_000)))
  timer.setEventHandler { fire.finish() }
  timer.resume()
  await withTaskCancellationHandler {
    for await _ in expired {}
  } onCancel: {
    timer.cancel()
    fire.finish()
  }
  try Task.checkCancellation()
}

func entered(_ hold: Hold) async -> Bool {
  await withTaskGroup(of: Bool.self) { group in
    group.addTask { await hold.entered(); return !Task.isCancelled }
    group.addTask { try? await expiry(.seconds(10)); return false }
    let arrived = await group.next() ?? false
    group.cancelAll()
    return arrived
  }
}

func valueBeforeDeadline<Value: Sendable>(_ task: Task<Value, any Error>, release: @escaping @Sendable () -> Void) async throws -> Value {
  try await withThrowingTaskGroup(of: Value.self) { group in
    group.addTask { try await task.value }
    group.addTask {
      try await expiry(.seconds(10))
      task.cancel()
      release()
      throw WaitFailure.timeout
    }
    defer { group.cancelAll() }
    return try await group.next()!
  }
}
let hangGuard: Duration = .seconds(20)

/// Returns at the next pulse of the tests or at the deadline. It never calls the core: a core that waits for the test must not hold the wait.
func untilPulse(after seen: Int, deadline: ContinuousClock.Instant) async {
  if TestPulse.shared.current != seen { return }
  await withTaskGroup(of: Void.self) { group in
    group.addTask { await TestPulse.shared.next(after: seen) }
    group.addTask { try? await expiry(ContinuousClock.now.duration(to: deadline)) }
    await group.next()
    group.cancelAll()
  }
}

final class OnceFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var taken = false
  func first() -> Bool { lock.withLock { defer { taken = true }; return !taken } }
}

/// Starts the listener and returns when its state is `.ready`; a failed or cancelled listener throws.
func startListening(_ listener: NWListener) async throws {
  let once = OnceFlag()
  try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
    listener.stateUpdateHandler = { state in
      switch state {
      case .ready: if once.first() { continuation.resume() }
      case .failed(let error): if once.first() { continuation.resume(throwing: error) }
      case .cancelled: if once.first() { continuation.resume(throwing: CancellationError()) }
      default: break
      }
    }
    listener.start(queue: .global())
  }
}
/// Waits for a condition on state that the tests observe: a read of an observable projection pulses at its change, the doubles and collectors pulse at theirs.
func eventually(
  _ what: String = "the condition",
  timeout: Duration = hangGuard,
  isolation: isolated (any Actor)? = #isolation,
  sourceLocation: SourceLocation = #_sourceLocation,
  _ condition: () -> Bool
) async {
  let deadline = ContinuousClock.now + timeout
  while ContinuousClock.now < deadline {
    let seen = TestPulse.shared.current
    if withObservationTracking({ condition() }, onChange: { TestPulse.shared.pulse() }) {
      return
    }
    await untilPulse(after: seen, deadline: deadline)
  }
  if condition() {
    return
  }
  Issue.record("timed out after \(timeout) waiting for \(what)", sourceLocation: sourceLocation)
}

func eventuallyAsync(
  _ what: String = "the condition",
  timeout: Duration = hangGuard,
  isolation: isolated (any Actor)? = #isolation,
  sourceLocation: SourceLocation = #_sourceLocation,
  _ condition: () async -> Bool
) async {
  let deadline = ContinuousClock.now + timeout
  while ContinuousClock.now < deadline {
    let seen = TestPulse.shared.current
    if await condition() {
      return
    }
    await untilPulse(after: seen, deadline: deadline)
  }
  if await condition() {
    return
  }
  Issue.record("timed out after \(timeout) waiting for \(what)", sourceLocation: sourceLocation)
}

/// The wait of a condition that reads: a read that throws fails the test with its error instead of reading as "not yet".
func eventuallyThrowing(
  _ what: String = "the condition",
  timeout: Duration = hangGuard,
  isolation: isolated (any Actor)? = #isolation,
  sourceLocation: SourceLocation = #_sourceLocation,
  _ condition: () async throws -> Bool
) async throws {
  var failure: (any Error)?
  await eventuallyAsync(what, timeout: timeout, sourceLocation: sourceLocation) {
    do { return try await condition() } catch { failure = error; return true }
  }
  if let failure { throw failure }
}

func consuming<Sequence: AsyncSequence & SendableMetatype>(
  _ cursor: Cursor<Sequence>,
  _ what: String,
  timeout: Duration = hangGuard,
  isolation: isolated (any Actor)? = #isolation,
  sourceLocation: SourceLocation = #_sourceLocation,
  until done: () -> Bool,
  apply: (Sequence.Element) -> Void
) async where Sequence.Element: Sendable, Sequence.AsyncIterator: SendableMetatype {
  while !done() {
    guard let item = await cursor.next(what, timeout: timeout, sourceLocation: sourceLocation) else { return }
    apply(item)
  }
}
final class Done: @unchecked Sendable {
  private let lock = NSLock()
  private var flag = false
  var value: Bool { lock.withLock { flag } }
  func set() {
    lock.withLock { flag = true }
    TestPulse.shared.pulse()
  }
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
func fencedUntil(_ what: String, isolation: isolated (any Actor)? = #isolation, fence: () async -> Void, sourceLocation: SourceLocation = #_sourceLocation, _ ready: () -> Bool) async {
  let deadline = ContinuousClock.now + .seconds(8)
  while !ready() {
    if ContinuousClock.now > deadline {
      Issue.record("timed out waiting for \(what)", sourceLocation: sourceLocation)
      return
    }
    await fence()
    await Task.yield()
  }
}
func parallel<Item: Sendable, Result: Sendable>(_ items: [Item], limit: Int, _ work: @escaping @Sendable (Item) async throws -> Result) async throws -> [Result] {
  try await withThrowingTaskGroup(of: (Int, Result).self) { group in
    var results: [(Int, Result)] = []
    var next = 0
    while next < min(limit, items.count) {
      let at = next
      group.addTask { (at, try await work(items[at])) }
      next += 1
    }
    while let done = try await group.next() {
      results.append(done)
      if next < items.count {
        let at = next
        group.addTask { (at, try await work(items[at])) }
        next += 1
      }
    }
    return results.sorted { $0.0 < $1.0 }.map(\.1)
  }
}

func drained(_ what: String, _ reader: ReadStream, timeout: Duration = hangGuard, sourceLocation: SourceLocation = #_sourceLocation) async throws -> [ReadValue] {
  let values = Collected()
  let finished = await finishes(what, timeout: timeout, sourceLocation: sourceLocation) {
    do {
      while let value = try await reader.pull() { values.append(value) }
    } catch {
      Issue.record("\(what) failed: \(error)", sourceLocation: sourceLocation)
    }
  }
  if !finished { await reader.cancel() }
  return values.all
}

final class Collected: @unchecked Sendable {
  private let lock = NSLock()
  private var values: [ReadValue] = []
  var all: [ReadValue] { lock.withLock { values } }
  func append(_ value: ReadValue) {
    lock.withLock { values.append(value) }
    TestPulse.shared.pulse()
  }
}

func nextElement<Element: Sendable>(of stream: AsyncStream<Element>) async -> Element? {
  var iterator = stream.makeAsyncIterator()
  return await iterator.next()
}

func parkUntilCancelled() async throws {
  let (parked, release) = AsyncStream<Void>.makeStream()
  await withTaskCancellationHandler {
    for await _ in parked {}
  } onCancel: {
    release.finish()
  }
  try Task.checkCancellation()
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
        try? await expiry(timeout)
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

func carriesRows(_ value: ReadValue) -> Bool {
  switch value {
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

struct TestLive<Model: DBModel>: Sendable {
  let query: Query<Model>

  func current() async throws -> [Model] { try await query.current() }

  func stream() async throws -> AsyncStream<[Model]> {
    let value = try AccountScope().stream(query)
    return AsyncStream { continuation in
      let task = Task {
        var projection = ListProjection<Model>()
        do {
          for try await next in value {
            let applied = try projection.apply(next)
            if applied, carriesRows(next) { continuation.yield(projection.rows) }
          }
        } catch {}
        continuation.finish()
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }
}

extension Query {
  var live: TestLive<Model> { TestLive(query: self) }
}

struct TestSnapshot<Model: DBModel>: Sendable {
  let rows: [Model]
  let phase: ReadPhase?
  let error: DbError?
  let total: Int?
  let hasMore: Bool
}

struct TestSnapshots<Model: DBModel>: Sendable {
  let query: Query<Model>

  func stream() async throws -> AsyncStream<TestSnapshot<Model>> {
    let value = try AccountScope().stream(query)
    return AsyncStream { continuation in
      let task = Task {
        var projection = ListProjection<Model>()
        do {
          for try await next in value {
            if try projection.apply(next) { continuation.yield(TestSnapshot(rows: projection.rows, phase: projection.phase, error: projection.state?.error, total: projection.state?.total.map { Int($0) }, hasMore: projection.state?.hasMore ?? false)) }
          }
        } catch {}
        continuation.finish()
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }
}

extension Query {
  var snapshots: TestSnapshots<Model> { TestSnapshots(query: self) }
}

struct TestCount<Model: DBModel>: Sendable {
  let query: Query<Model>
  func current() async throws -> Int { try await query.current().count }
}

extension Query {
  var count: TestCount<Model> { TestCount(query: self) }
}

extension ReadState {
  /// The first load ended and the relation has no next page.
  var complete: Bool { settled && !hasMore && error == nil }
}

struct TestListState<Model: DBModel>: Sendable {
  let phase: ReadPhase?
  let total: Int?
  let hasMore: Bool
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
    return TestListState(phase: state?.phase, total: state?.total.map { Int($0) }, hasMore: state?.hasMore ?? false, complete: state?.complete ?? false, error: state?.error, rows: projected.rows)
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

/// A reader of the events of a model once its source is open: the first item it read was `.ready`.
func opened<Model: DBModel>(_ events: EventStream<Model>, sourceLocation: SourceLocation = #_sourceLocation) async -> Cursor<EventStream<Model>> {
  let cursor = Cursor(events)
  guard case .ready? = await cursor.next("the source of the events opens", sourceLocation: sourceLocation) else {
    Issue.record("the first item of the events stream was not ready", sourceLocation: sourceLocation)
    return cursor
  }
  return cursor
}

/// The network goes away while the held request is at the server: the load waits (Loading x Online(false) is Waiting) until the test sets the network online again.
@MainActor
func networkDrops(during held: Hold) async throws {
  await held.entered()
  try DB.database.setOnline(online: false)
}

extension Projected {
  /// Returns when the projection shows a settled state, throws the failed state's error or `Interrupted` when the stream ended. Bounded by `hangGuard`.
  func loaded() async throws {
    var result: Result<Void, any Error>?
    await eventually("the projection shows a settled state") {
      if let reason = projection.ended, reason != .invalid {
        result = .failure(DbError.Interrupted(detail: "the stream ended before its rows were shown"))
        return true
      }
      guard let state = projection.state, state.settled else { return false }
      if state.phase == .failed, let error = state.error { result = .failure(error) } else { result = .success(()) }
      return true
    }
    try result?.get()
  }
}
