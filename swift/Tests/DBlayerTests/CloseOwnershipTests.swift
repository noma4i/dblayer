import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func closingTheDatabaseEndsAWaitingChangesStreamAndStopsWatchingThePath() async throws {
    let path = FakePath()
    defer { Task { @MainActor in try? await DB.close() } }
    try await DB.close()
    try await Reachability.$source.withValue(path) {
      try await DB.openTesting(transport: MockTransport(), modelsJSON: modelsJSON)
    }
    try DB.database.setOnline(online: false)
    #expect(path.observerCount == 1)
    _ = try await Moment.send(in: DB.account(), text: "kept").optimisticValue!
    let cursor = Cursor(try await Moment.recent.live.stream())
    #expect(await cursor.next("the initial delivery") != nil)
    let waiting = Task { await cursor.next("the end of the stream after the close") }
    await Task.yield()
    try await DB.close()
    let after = await waiting.value
    #expect(after == nil)
    #expect(path.observerCount == 0)
  }
}
