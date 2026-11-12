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

private final class RowHistory: @unchecked Sendable {
  private let lock = NSLock()
  private var projection = ListProjection<User>()
  private var history: [[String]] = []

  var states: [[String]] { lock.withLock { history } }

  func begin() {
    lock.withLock { projection = ListProjection<User>() }
    TestPulse.shared.pulse()
  }

  func take(_ value: ReadValue) {
    lock.withLock {
      if (try? projection.apply(value)) == true { history.append(projection.rows.map(\.name)) }
    }
    TestPulse.shared.pulse()
  }
}

private final class HeldList: @unchecked Sendable {
  let stream: ValueStream
  let reading: Task<Void, Never>

  init(_ stream: ValueStream, _ history: RowHistory) {
    self.stream = stream
    history.begin()
    reading = Task {
      do { for try await value in stream { history.take(value) } } catch {}
    }
  }

  func cancel() async {
    reading.cancel()
    await stream.cancel()
  }
}

private final class Outcome: @unchecked Sendable {
  private let lock = NSLock()
  private var finished = false
  var isDone: Bool { lock.withLock { finished } }
  func finish() {
    lock.withLock { finished = true }
    TestPulse.shared.pulse()
  }
}

private struct LoginLink: Encodable, Sendable { let account: String }
private struct NoBody: Encodable, Sendable {}

/// The tests share `DB.database` with the rest of `BridgeProbeTests`, so they run in its serialized suite.
extension BridgeProbeTests {
  private static let account = SignedAccount()

  @Test @MainActor
  func bridge_prop_session_state_parity_c9() async throws {
    await DB.closeForCleanup()
    do { _ = try DB.sessionStatus(); Issue.record("status before open succeeded") }
    catch DbError.NotOpen(_) {} catch DbError.Closed(_) {} catch { Issue.record("before open: \(error)") }
    _ = try await Self.openSealedSession()
    let initial = try DB.sessionStatus()
    #expect(initial.phase == .none && initial.identity == nil)
    try await Self.startSealedSession("A")
    let active = try DB.sessionStatus()
    #expect(active.phase == .active && active.identity != nil && active.epoch > initial.epoch)
    try await DB.sessionReset()
    let reset = try DB.sessionStatus()
    #expect(reset.phase == .none && reset.identity == nil && reset.epoch > active.epoch)
    try await DB.close()
    do { _ = try DB.sessionStatus(); Issue.record("status after close succeeded") }
    catch DbError.Closed(_) {} catch { Issue.record("after close: \(error)") }
  }

  @MainActor
  static func openSealedSession() async throws -> SealedServer {
    await DB.closeForCleanup()
    let account = Self.account
    account.signOut()
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
    let encrypted = EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
    try await DB.openTesting(transport: MockTransport(), modelsJSON: withRawCalls(modelsJSON), encrypted: encrypted, credentials: LifecycleCredential(), http: server)
    let fixtureOperations = #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"},{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}]"#
    try server.registerDocuments(fixtureOperations)
    try await registerStandOperations(DB.database, defsJson: fixtureOperations)
    return server
  }

  fileprivate static func holdList(_ history: RowHistory) throws -> HeldList {
    HeldList(try DB.account().stream(Query<User>()), history)
  }

  /// The order of the app: sign-in call, session profile, profile refresh.
  @MainActor
  static func startSealedSession(_ account: String) async throws {
    try await DB.sessionCall("login_link", body: LoginLink(account: account))
    try await DB.call("SessionProfile", variables: [:])
    try await User.profile.refresh()
  }

  /// The real exit of the app: the confirmed logout call, the session reset, the data clear, then the next login. No read follows the exit and account A never shows after it.
  @MainActor
  @Test
  func aConfirmedLogoutThenASessionResetAndAClearNeverShowsTheAccountToTheNextLogin() async throws {
    let server = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    var entity = Projected<User>(try DB.account().stream(User.profile))
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    try await DB.sessionCall("logout", body: NoBody())
    try await DB.sessionReset()
    #expect(entity.projection.ended == .sessionEnded)
    let before = server.requests.count
    server.refuseEverything(true)
    #expect(server.requests.count == before)
    server.refuseEverything(false)
    try await Self.startSealedSession("B")
    entity = Projected<User>(try DB.account().stream(User.profile))
    await eventually(#"entity.value?.name == "B""#) { entity.value?.name == "B" }
  }

  /// Every value delivered to a list held across a logout and the next login: none shows account A after the logout and the last one shows B.
  @MainActor
  @Test
  func aLoginAfterALogoutNeverDeliversTheLastAccountToAHeldList() async throws {
    _ = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let history = RowHistory()
    let live = try Self.holdList(history)
    defer { live.reading.cancel() }
    await eventually("the held list shows A") { history.states.last?.contains("A") == true }
    try await DB.sessionReset()
    let seen = history.states.count
    try await Self.startSealedSession("B")
    _ = try Self.holdList(history)
    await eventually("the held list shows B") { history.states.last?.contains("B") == true }
    let after = history.states.dropFirst(seen)
    #expect(after.allSatisfy { !$0.contains("A") }, "account A came back after the logout: \(after)")
  }

  /// C11: a call that the server never answers ends when the database closes.
  @MainActor
  @Test
  func closeEndsACallThatTheServerNeverAnswers() async throws {
    let server = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    server.hold(operation: "UserProfile")
    let ended = Outcome()
    Task { @MainActor in
      _ = try? await User.profile.refresh()
      ended.finish()
    }
    await eventually("the request waits at the server") { server.parkedCount > 0 }
    await DB.closeForCleanup()
    await eventually("the call ended without its answer") { ended.isDone }
    server.hold(operation: nil)
  }
}
