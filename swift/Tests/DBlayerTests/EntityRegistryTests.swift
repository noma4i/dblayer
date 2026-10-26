import Foundation
import Observation
import Testing
@testable import DBlayer

final class CountingSource: EntitySource, @unchecked Sendable {
  private let cached: WireRow?
  init(cached: WireRow? = nil) {
    self.cached = cached
    (events, eventSink) = AsyncStream.makeStream()
  }
  func cachedRow(_ id: String) -> WireRow? { lock.withLock { peekCount += 1 }; return cached?.id == id ? cached : nil }
  private let lock = NSLock()
  private var peekCount = 0
  var peeks: Int { lock.withLock { peekCount } }
  private var retains: [[String]] = []
  private var releases: [[String]] = []
  /// Each lease call in order, as "retain <ids>" or "release <ids>".
  let events: AsyncStream<String>
  private let eventSink: AsyncStream<String>.Continuation
  func retain(_ ids: [String]) { lock.withLock { retains.append(ids) }; eventSink.yield("retain \(ids.joined(separator: ","))") }
  func release(_ ids: [String]) { lock.withLock { releases.append(ids) }; eventSink.yield("release \(ids.joined(separator: ","))") }
  var retained: [String] { lock.withLock { retains.flatMap { $0 } } }
  var released: [String] { lock.withLock { releases.flatMap { $0 } } }
}

final class Counter: @unchecked Sendable {
  private let lock = NSLock()
  private var count = 0
  var value: Int { lock.withLock { count } }
  func add(_ amount: Int = 1) { lock.withLock { count += amount } }
}

final class Clock: @unchecked Sendable {
  private let lock = NSLock()
  private var seconds: TimeInterval = 0
  var now: TimeInterval { lock.withLock { seconds } }
  func advance(_ amount: TimeInterval) { lock.withLock { seconds += amount } }
}

@MainActor
struct EntityRegistryTests {
  func moment(_ id: String, _ text: String) -> Moment {
    Moment(id: id, uuid: id, text: text, media: .object([:]), createdAt: "2026-10-08T00:00:00.000Z")
  }

  func delivery(_ revision: Int64, _ rows: [Moment] = [], removed: [String] = []) -> EntityDecoding.Decoded<Moment> {
    EntityDecoding.Decoded(revision: revision, rows: rows.map { ($0, 1) }, removed: removed)
  }

  func settle() async { for _ in 0 ..< 5 { await Task.yield() } }

  /// REL-01
  @Test
  func oneValuePerIdForEveryReader() async {
    let registry = Entities<Moment>(source: CountingSource())
    let readers = (0 ..< 3).map { _ in registry.entity("1") }
    registry.apply(delivery(1, [moment("1", "A")]))
    registry.apply(delivery(2, [moment("1", "B")]))
    #expect(readers.allSatisfy { $0.value?.text == "B" })
    #expect(Set(readers.map(ObjectIdentifier.init)).count == 1)
    #expect(registry.liveCount == 1)
  }

  /// REL-04
  @Test
  func lowerRevisionNeverReplacesHigher() {
    let registry = Entities<Moment>(source: CountingSource())
    let entity = registry.entity("1")
    registry.apply(delivery(7, [moment("1", "seven")]))
    registry.apply(delivery(5, [moment("1", "five")]))
    #expect(entity.value?.text == "seven")
    registry.apply(delivery(9, [moment("1", "nine")]))
    registry.apply(delivery(8, removed: ["1"]))
    #expect(entity.value?.text == "nine" && entity.revision == 9)
  }

  /// REL-05
  @Test
  func releasedLeaseNeverWritesAValue() async {
    let registry = Entities<Moment>(source: CountingSource())
    var entity: Entity<Moment>? = registry.entity("1")
    registry.apply(delivery(1, [moment("1", "old")]))
    #expect(entity?.value?.text == "old")
    entity = nil
    registry.apply(delivery(2, [moment("1", "new")]))
    let again = registry.entity("1")
    #expect(again.value?.text == "old" && again.isStale)
    registry.apply(delivery(0, [moment("1", "late")]))
    #expect(again.value?.text == "old")
    registry.apply(delivery(3, [moment("1", "fresh")]))
    #expect(again.value?.text == "fresh" && !again.isStale)
  }

  /// REL-09
  @Test
  func oneChangedIdInvalidatesOnlyItsReaders() {
    let registry = Entities<Moment>(source: CountingSource())
    let entities = (0 ..< 10).map { registry.entity("\($0)") }
    registry.apply(delivery(1, (0 ..< 10).map { moment("\($0)", "first") }))
    let invalidated = Counter()
    func read(_ entity: Entity<Moment>) {
      withObservationTracking { _ = entity.value } onChange: { invalidated.add() }
    }
    for index in 0 ..< 50 { read(entities[index % 10]) }
    registry.apply(delivery(2, [moment("3", "second")]))
    #expect(invalidated.value == 5)
  }

  /// REL-10
  @Test
  func secondReaderReadsWarmValueSynchronously() async {
    let source = CountingSource()
    let registry = Entities<Moment>(source: source)
    let first = registry.entity("1")
    registry.apply(delivery(1, [moment("1", "warm")]))
    await settle()
    let retainsBefore = source.retained
    let second = registry.entity("1")
    await settle()
    #expect(second.value?.text == "warm" && second === first)
    #expect(source.retained == retainsBefore && retainsBefore == ["1"])
  }

  /// REL-11
  @Test
  func retainedValueIsDroppedAfterTheBound() async {
    let clock = Clock()
    let registry = Entities<Moment>(now: { clock.now }, source: CountingSource())
    for index in 0 ..< 3 {
      var entity: Entity<Moment>? = registry.entity("\(index)")
      registry.apply(delivery(1, [moment("\(index)", "v")]))
      #expect(entity?.value != nil)
      entity = nil
    }
    #expect(registry.retainedCount == 3)
    let shown = registry.entity("1")
    #expect(shown.isStale && shown.value?.text == "v")
    clock.advance(3600)
    #expect(registry.retainedCount == 0)
  }

  /// REL-24
  @Test
  func resetDropsRetainedValuesAndKeepsEveryLeaseOfTheHeldEntities() async {
    let source = CountingSource()
    let registry = Entities<Moment>(source: source)
    let held = (0 ..< 5).map { registry.entity("\($0)") }
    var dropped: [Entity<Moment>]? = (5 ..< 10).map { registry.entity("\($0)") }
    registry.apply(delivery(1, (0 ..< 10).map { moment("\($0)", "v") }))
    #expect(dropped?.count == 5)
    dropped = nil
    #expect(registry.retainedCount == 5)
    registry.reset()
    await settle()
    #expect(registry.retainedCount == 0 && registry.liveCount == 5)
    #expect(Set(source.released) == Set((5 ..< 10).map(String.init)))
    registry.apply(EntityDecoding.Decoded(revision: 2, rows: [], removed: (0 ..< 5).map(String.init)))
    #expect(held.allSatisfy { $0.value == nil && !$0.isClosed })
    registry.apply(delivery(3, (0 ..< 5).map { moment("\($0)", "next") }))
    #expect(held.allSatisfy { $0.value?.text == "next" })
  }

  /// Addendum 3a: a cancelled attach leaves no subscription, at each of the 3 timings.
  @Test
  func attachCancelledAtAnyMomentLeavesNoSubscription() async throws {
    let active = Counter()
    func blocking(_ gate: DispatchSemaphore) -> LiveSource<Int> {
      LiveSource<Int>(initial: 0) { emit in
        active.add()
        gate.wait()
        emit(1)
        return { active.add(-1) }
      }
    }
    let beforeGate = DispatchSemaphore(value: 0)
    let before = Task { try await blocking(beforeGate).stream() }
    before.cancel()
    beforeGate.signal()
    _ = try await before.value
    await eventually("the attach that began cancelled is released", timeout: .seconds(5)) { active.value == 0 }

    let duringGate = DispatchSemaphore(value: 0)
    let during = Task { try await blocking(duringGate).stream() }
    await eventually("the attach is running") { active.value == 1 }
    during.cancel()
    duringGate.signal()
    _ = try await during.value
    await eventually("the attach cancelled while running is released", timeout: .seconds(5)) { active.value == 0 }

    let immediate = LiveSource<Int>(initial: 0) { emit in
      active.add()
      emit(1)
      return { active.add(-1) }
    }
    let after = Task { _ = await Cursor(try await immediate.stream()).next("the first element") }
    try await after.value
    await eventually("the attach cancelled afterwards is released", timeout: .seconds(5)) { active.value == 0 }
  }

  enum Replacements: Sendable { case independent, chain }

  /// PERF-16: a slow reader gets one pending result that holds every replacement pair of the burst, a chain of replacements reads as one pair.
  @Test(arguments: [(Replacements.independent, 10), (.independent, 100), (.independent, 1000), (.chain, 1000)])
  func slowConsumerStreamKeepsOnePendingResultAndEveryReplacementPair(shape: Replacements, burst: Int) async throws {
    let source = LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:]), merge: { Snapshot.merging($0, $1) }) { emit in
      for step in 0 ..< burst { emit(Snapshot(rows: [], replaced: shape == .chain ? ["id\(step + 1)": "id\(step)"] : ["real\(step)": "temp\(step)"])) }
      return {}
    }
    let stream = try await source.stream()
    let merged = await Cursor(stream).next()
    switch shape {
    case .independent: #expect(merged?.replaced.count == burst)
    case .chain: #expect(merged?.replaced == ["id\(burst)": "id0"])
    }
    #expect(!stream.hasPending)
  }
}
