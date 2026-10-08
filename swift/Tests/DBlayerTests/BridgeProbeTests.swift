import Testing
@testable import DBlayer

struct BridgeProbeTests {
  @Test
  func writesReachTypedLiveRows() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    var changes = Moment.recent.live.stream.makeAsyncIterator()
    #expect(await changes.next()?.isEmpty == true)
    let moment = try await Moment.create(uuid: "m", createdAt: "2026-10-08T00:00:00.000Z")
    #expect(await changes.next()?.first?.uuid == "m")
    _ = try await moment.update(text: "edited")
    #expect(await changes.next()?.first?.text == "edited")
    try await moment.destroy()
    #expect(await changes.next()?.isEmpty == true)
  }
}
