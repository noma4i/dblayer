import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func aClosedDatabaseRefusesEveryCallWithTheClosedErrorOfTheCore() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON)
    try await DB.close()
    func refused(_ call: () async throws -> Void) async -> Bool {
      do { try await call() } catch DbError.Closed(_) { return true } catch { return false }
      return false
    }
    #expect(await refused { _ = try await Moment.recent.current() })
    do { try await DB.close() } catch DbError.Closed(_) { return } catch { Issue.record("second close: \(error)") }
    Issue.record("second close succeeded")
  }
}
