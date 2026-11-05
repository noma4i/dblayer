import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func aClosedDatabaseRefusesEveryCallWithTheClosedErrorOfTheCore() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON)
    let row = try await Moment.send(in: DB.account(), text: "saved").optimisticValue!
    try await DB.close()
    func refused(_ call: () async throws -> Void) async -> Bool {
      do { try await call() } catch DbError.Closed(_) { return true } catch { return false }
      return false
    }
    #expect(await refused { try await row.destroy() })
    #expect(await refused { _ = try await Moment.recent.current() })
    try await DB.close()
  }
}
