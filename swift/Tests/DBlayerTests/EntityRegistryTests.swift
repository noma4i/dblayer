import Foundation
import Testing
@testable import DBlayer

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
