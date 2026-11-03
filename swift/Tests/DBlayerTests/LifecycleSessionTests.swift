import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class LifecycleCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?

  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private final class SignedAccount: @unchecked Sendable {
  private let lock = NSLock()
  private var name: String?

  var current: String? { lock.withLock { name } }
  func signIn(_ account: String) { lock.withLock { name = account } }
  func signOut() { lock.withLock { name = nil } }
}

private struct LoginLink: Encodable, Sendable { let account: String }
private struct NoBody: Encodable, Sendable {}

/// The tests share `DB.database` with the rest of `BridgeProbeTests`, so they run in its serialized suite.
extension BridgeProbeTests {
  @MainActor
  static func openSealedSession() async throws -> SealedServer {
    DB.close()
    let account = SignedAccount()
    let server = try SealedServer { path, body in
      if path == "/api/auth/login_link" {
        let name = body["account"] as? String ?? ""
        account.signIn(name)
        return ["sessionId": name]
      }
      if path == "/api/logout" {
        account.signOut()
        return ["revoked": true]
      }
      guard let name = account.current else { return ["error": "UNAUTHENTICATED"] }
      switch body["operationName"] as? String {
      case "SessionProfile": return ["data": ["sessionProfile": ["id": name]]]
      case "UserProfile": return ["data": ["me": ["__typename": "User", "id": "1", "name": name]]]
      default: return ["errors": [["message": "unexpected operation"]]]
      }
    }
    let encrypted = EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", sessionless: false)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
    try await DB.openTesting(transport: MockTransport(), modelsJSON: modelsJSON, encrypted: encrypted, credentials: LifecycleCredential(), http: server)
    try DB.database.registerOperations(defsJson: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"},{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}]"#)
    return server
  }

  /// The order of the app: sign-in call, data logout, session profile, profile refresh.
  @MainActor
  static func startSealedSession(_ account: String) async throws {
    try await DB.sessionCall("login_link", body: LoginLink(account: account))
    try await DB.logout()
    try await DB.run("SessionProfile", variables: [:])
    try await User.profile.refresh()
  }

  /// REL-24 and the login hang: a login after a logout refreshes the held entity, no read reaches the server after the logout, and the next account never shows the last one.
  @MainActor
  @Test
  func anAppLoginAfterALogoutRefreshesTheHeldEntityAndNoReadFollowsTheLogout() async throws {
    let server = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    let before = server.requests.count
    try await DB.logout()
    #expect(entity.value == nil)
    server.refuseEverything(true)
    try await DB.resetForTesting()
    try await DB.settleForTesting()
    #expect(server.requests.count == before)
    server.refuseEverything(false)
    try await Self.startSealedSession("B")
    await eventually(#"entity.value?.name == "B""#) { entity.value?.name == "B" }
  }

  /// The real exit of the app: the confirmed logout call, the session reset, the data clear, then the next login. No read follows the exit and account A never shows after it.
  @MainActor
  @Test
  func aConfirmedLogoutThenASessionResetAndAClearNeverShowsTheAccountToTheNextLogin() async throws {
    let server = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    try await DB.sessionCall("logout", body: NoBody())
    try DB.sessionReset()
    try await DB.logout()
    #expect(entity.value == nil)
    let before = server.requests.count
    server.refuseEverything(true)
    try await DB.settleForTesting()
    #expect(server.requests.count == before)
    server.refuseEverything(false)
    try await Self.startSealedSession("B")
    await eventually(#"entity.value?.name == "B""#) { entity.value?.name == "B" }
  }
}
