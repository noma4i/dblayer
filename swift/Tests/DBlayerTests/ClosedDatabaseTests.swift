import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func aClosedDatabaseRefusesEveryCallWithTheNotOpenError() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "saved")
    try await DB.close()
    let message = "database is not open; call DBlayer.open once at app start"
    func refused(_ call: () async throws -> Void) async -> Bool {
      do { try await call() } catch let error as DatabaseNotOpen { return error.description == message } catch { return false }
      return false
    }
    #expect(await refused { _ = try DB.database })
    #expect(await refused { _ = try DB.subscriptionCensus() })
    #expect(await refused { _ = try await Moment.create(text: "late") })
    #expect(await refused { try await row.destroy() })
    #expect(await refused { try await Moment.recent.warm() })
    #expect(await refused { _ = try await Moment.recent.live.stream() })
    #expect(await refused { _ = try await Moment.recent.snapshots.stream() })
    #expect(await refused { _ = try await Moment.recent.changes.stream() })
    #expect(await refused { _ = try await Moment.recent.live.current() })
  }

  @Test @MainActor func aSingletonOfAClosedDatabaseIsClosed() async throws {
    try await DB.close()
    let entity = User.entity()
    await eventually("the singleton of a closed database closes") { entity.isClosed }
    #expect(entity.value == nil)
  }
}
