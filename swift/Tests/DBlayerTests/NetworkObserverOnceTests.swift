import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class SilentHttp: ScriptedHttp, Sendable {
  func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply { throw CancellationError() }
}

extension BridgeProbeTests {
  /// net_observer_starts_once_for_database_and_config: `NetworkStatus()`, `serverConfig` and `open` in one process leave the one network observer the first status started.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_observer_starts_once_for_database_and_config() async throws {
    _ = StatusProbe.status()
    _ = NetworkStatus()
    let encrypted = EncryptedConfig(origin: "https://example.com", serverKey: "", pin: "", endpoints: EncryptedEndpoints(graphql: "/graphql", cable: "/cable", logout: "/logout", bootstrap: "/bootstrap", sessionOperations: [:]), protocolLabel: "test-v1", cablePurpose: "hello", cableChannel: "TestChannel")
    let config = Task { try? await DB.serverConfig(encrypted: encrypted, platform: .ios, version: "1.2", http: SilentHttp()) }
    config.cancel()
    await config.value
    await expectDbError("Invalid") { try await DB.open(modelsJSON: "invalid") }
    #expect(StatusProbe.shared.starts == 1)
  }
}
