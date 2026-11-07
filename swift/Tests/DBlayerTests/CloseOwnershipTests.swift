import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func closingTheDatabaseEndsAWaitingChangesStreamAndStopsWatchingThePath() async throws {
    let path = FakePath()
    defer { Task { @MainActor in try? await DB.close() } }
    await DB.closeForCleanup()
    try await reopenClocked(modelsJson: modelsJSON, transport: MockTransport(), observers: SystemObservers(path: path))
    try DB.database.setOnline(online: false)
    #expect(path.observerCount == 1)
    let cursor = Cursor(try await Moment.recent.live.stream())
    #expect(await cursor.next("the initial delivery") != nil)
    let waiting = Task { await cursor.next("the end of the stream after the close") }
    await Task.yield()
    await DB.closeForCleanup()
    let after = await waiting.value
    #expect(after == nil)
    #expect(path.observerCount == 0)
  }
}
