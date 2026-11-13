import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private struct HeaderLogin: Encodable, Sendable { let account: String }

private final class HeaderCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private final class Token: @unchecked Sendable {
  private let lock = NSLock()
  private var current = "first"
  var value: String {
    get { lock.withLock { current } }
    set { lock.withLock { current = newValue } }
  }
}

extension BridgeProbeTests {
struct OpenHeadersTests {
  init() { _ = StatusProbe.status() }

  @Test @MainActor
  func openReadsTheHeadersAgainForEveryRequest() async throws {
    await DB.closeForCleanup()
    let server = try BootstrapListener(responseStatus: "400 Bad Request")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let token = Token()
    let encrypted = EncryptedConfig(origin: origin.absoluteString, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
    try await DB.open(headers: { ["X-Token": token.value] }, encrypted: encrypted, credentials: HeaderCredential(), modelsJSON: emptySpecs)

    try await DB.database.setOnline(online: true)
    await expectDbError("Http") { try await DB.sessionCall("login_link", body: HeaderLogin(account: "A")) }
    token.value = "second"
    await expectDbError("Http") { try await DB.sessionCall("login_link", body: HeaderLogin(account: "A")) }

    let tokens = server.requests.compactMap { request in
      request.components(separatedBy: "\r\n").first { $0.lowercased().hasPrefix("x-token:") }.map { $0.split(separator: ":", maxSplits: 1)[1].trimmingCharacters(in: .whitespaces) }
    }
    #expect(tokens.first == "first" && tokens.last == "second" && tokens.count >= 2, "tokens sent: \(tokens)")
    let agents = server.requests.compactMap { request in
      request.components(separatedBy: "\r\n").first { $0.lowercased().hasPrefix("user-agent:") }.map { $0.split(separator: ":", maxSplits: 1)[1].trimmingCharacters(in: .whitespaces) }
    }
    #expect(agents.count == server.requests.count && agents.allSatisfy { $0 == userAgent(application: nil) }, "agents sent: \(agents)")
  }
}
}
