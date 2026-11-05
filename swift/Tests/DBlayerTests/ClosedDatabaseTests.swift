import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func aClosedDatabaseRefusesEveryCallWithTheNotOpenError() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON)
    let row = try await Moment.send(in: DB.account(), text: "saved").optimisticValue!
    try await DB.close()
    let message = "database is not open; call DBlayer.open once at app start"
    func refused(_ call: () async throws -> Void) async -> Bool {
      do { try await call() } catch let error as DatabaseNotOpen { return error.description == message } catch { return false }
      return false
    }
    #expect(await refused { _ = try DB.database })
    #expect(await refused { _ = try DB.subscriptionCensus() })
    #expect(await refused { _ = try DB.account() })
    #expect(await refused { try await row.destroy() })
    #expect(await refused { _ = try await Moment.recent.current() })
  }
}
