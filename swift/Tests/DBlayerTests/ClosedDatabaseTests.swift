import DBlayerTesting
import Testing
@_spi(Testing) @testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func aClosedDatabaseRefusesEveryCallWithTheClosedErrorOfTheCore() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    try await DB.close()
    func refused(_ call: () async throws -> Void) async -> Bool {
      do { try await call() } catch DbError.Closed(_) { return true } catch { return false }
      return false
    }
    #expect(await refused { _ = try await Moment.recent.current() })
    do { try await DB.close() } catch { Issue.record("a second close answers success: \(error)") }
  }
}

extension BridgeProbeTests {
  @Test @MainActor func life_prop_not_open_typed() async throws {
    func code(_ call: () async throws -> Void) async -> String {
      do { try await call() } catch DbError.NotOpen(_) { return "not_open" } catch DbError.Closed(_) { return "closed" } catch { return "other \(error)" }
      return "none"
    }
    func entries() async -> [String] {
      [
        await code { _ = try await DB.call("Probe", variables: [:], as: JSONValue.self) },
        await code { try await DB.call("Probe", variables: [:]) },
        await code { try await DB.sessionCall("Probe", body: JSONValue.null) },
        await code { _ = try await DB.sessionCall("Probe", body: JSONValue.null, as: JSONValue.self) },
        await code { try await DB.sessionReset() },
        await code { _ = try await DB.subscriptionCensus() },
        await code { _ = try DB.openReport() },
        await code { _ = try DB.account() },
        await code { _ = try await Moment.recent.live.current() },
        await code { try await DB.close() },
      ]
    }
    await DB.closeForCleanup()
    #expect(await code { _ = try OpenDatabase().opened() } == "not_open")
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    try await DB.close()
    let afterClose = await entries()
    let gated = (0...8).map { afterClose[$0] }
    #expect(gated == Array(repeating: "closed", count: 9), "\(afterClose)")
    #expect(afterClose[9] == "none", "a second close answers success")
  }
}

extension BridgeProbeTests {
  @Test @MainActor func aQueryWithoutARelationIsInvalidFromTheCore() async throws {
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: MockTransport())
    func invalid(_ call: () async throws -> Void) async -> Bool {
      do { try await call() } catch DbError.Invalid(_) { return true } catch { return false }
      return false
    }
    #expect(await invalid { try await Query<Moment>().refresh() })
    #expect(await invalid { try await Query<Moment>().load() })
    await DB.closeForCleanup()
  }
}
