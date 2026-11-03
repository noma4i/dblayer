import Foundation
import Testing
@testable import DBlayer

private final class Probe: @unchecked Sendable {}

private final class Calls: @unchecked Sendable {
  private let lock = NSLock()
  private var count = 0
  var value: Int { lock.withLock { count } }
  func add() { lock.withLock { count += 1 } }
}

struct OnlineRelayDetachTests {
  @Test func detachingTheSinkReleasesItAndStopsDelivery() {
    let relay = OnlineRelay { _ in }
    let calls = Calls()
    weak var held: Probe?
    do {
      let probe = Probe()
      held = probe
      relay.attach { _ in withExtendedLifetime(probe) {}; calls.add() }
    }
    #expect(held != nil)
    let before = calls.value
    relay.detach()
    #expect(held == nil)
    relay.set(true)
    #expect(calls.value == before)
  }
}

extension BridgeProbeTests {
  @Test @MainActor func closingTheDatabaseEndsAWaitingChangesStreamAndDetachesTheSink() async throws {
    let path = Reachability(monitor: nil)
    DB.reachability = path
    defer {
      DB.reachability = .shared
      Task { @MainActor in try? await DB.close() }
    }
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: nil)
    _ = try await Moment.create(text: "kept")
    let cursor = Cursor(try await Moment.recent.changes.stream())
    #expect(await cursor.next("the initial delivery") != nil)
    let waiting = Task { await cursor.next("the end of the stream after the close") }
    await Task.yield()
    try await DB.close()
    let after = await waiting.value
    #expect(after == nil)
    path.changed(true)
  }
}
