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

private final class RowHistory: QueryListener, @unchecked Sendable {
  private let lock = NSLock()
  private var rows: [WireRow] = []
  private var delivered: [[String]] = []

  var states: [[String]] { lock.withLock { delivered } }

  func changed(change: LiveChange) {
    lock.withLock {
      if change.full {
        rows = change.rows
      } else {
        rows.removeAll { change.removed.contains($0.id) }
        for row in change.rows {
          rows.removeAll { $0.id == row.id }
          rows.append(row)
        }
      }
      delivered.append(rows.flatMap { row in row.cells.compactMap { if case let .text(value) = $0 { value } else { nil } } })
    }
  }
}

private final class Outcome: @unchecked Sendable {
  private let lock = NSLock()
  private var finished = false
  var isDone: Bool { lock.withLock { finished } }
  func finish() { lock.withLock { finished = true } }
}

private final class OneShot: @unchecked Sendable {
  private let lock = NSLock()
  private var armed = false
  func arm() { lock.withLock { armed = true } }
  func take() -> Bool { lock.withLock { defer { armed = false }; return armed } }
}

private struct LoginLink: Encodable, Sendable { let account: String }
private struct NoBody: Encodable, Sendable {}

/// The tests share `DB.database` with the rest of `BridgeProbeTests`, so they run in its serialized suite.
extension BridgeProbeTests {
  private static let account = SignedAccount()
  private static let alreadyAuthenticated = OneShot()

  @MainActor
  static func openSealedSession() async throws -> SealedServer {
    try await DB.close()
    let account = Self.account
    account.signOut()
    let server = try SealedServer { path, body in
      if path == "/api/auth/login_link" {
        if Self.alreadyAuthenticated.take() { return ["error": "ALREADY_AUTHENTICATED"] }
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

  /// The order of the app: sign-in call, session profile, profile refresh.
  @MainActor
  static func startSealedSession(_ account: String) async throws {
    try await DB.sessionCall("login_link", body: LoginLink(account: account))
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
    try await DB.sessionReset()
    #expect(entity.value == nil)
    server.refuseEverything(true)
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
    try await DB.sessionReset()
    #expect(entity.value == nil)
    let before = server.requests.count
    server.refuseEverything(true)
    #expect(server.requests.count == before)
    server.refuseEverything(false)
    try await Self.startSealedSession("B")
    await eventually(#"entity.value?.name == "B""#) { entity.value?.name == "B" }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theAppExitOrderEndsEveryHeldWaitWhenTheCredentialIsGone() async throws {
    _ = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    let changes = try DB.database.changes(queryJson: #"{"model":"User"}"#)
    let holding = Task { while (try? await changes.next()) != nil {} }
    try await DB.sessionCall("logout", body: NoBody())
    try await DB.sessionReset()
    holding.cancel()
    changes.cancel()
    await holding.value
    try await DB.sessionReset()
    await #expect(throws: DbError.self) { try await entity.waitForInitialLoad() }
  }

  /// Every value delivered to a list held across a logout and the next login: none shows account A after the logout and the last one shows B.
  @MainActor
  @Test
  func aLoginAfterALogoutNeverDeliversTheLastAccountToAHeldList() async throws {
    _ = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let history = RowHistory()
    let live = try DB.database.liveQuery(queryJson: #"{"model":"User"}"#, listener: history)
    defer { live.cancel() }
    await eventually("the held list shows A") { history.states.last?.contains("A") == true }
    let seen = history.states.count
    try await DB.sessionReset()
    try await Self.startSealedSession("B")
    await eventually("the held list shows B") { history.states.last?.contains("B") == true }
    let after = history.states.dropFirst(seen)
    #expect(after.allSatisfy { !$0.contains("A") }, "account A came back after the logout: \(after)")
  }

  /// C06, R-6: the iOS exit with a wait for the first load that the reset left pending at the server. Every call of the exit returns, the wait ends, the held entity is empty at once, and the late answer of account A shows nowhere.
  @MainActor
  @Test
  func theAppExitOrderEndsAPendingInitialWaitAndEveryCallOfTheExit() async throws {
    let server = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    let history = RowHistory()
    let live = try DB.database.liveQuery(queryJson: #"{"model":"User"}"#, listener: history)
    defer { live.cancel() }
    await eventually("the held list shows A") { history.states.last?.contains("A") == true }
    server.hold(operation: "UserProfile")
    Task { @MainActor in _ = try? await User.profile.refresh() }
    await eventually("the reread waits at the server") { server.parkedCount > 0 }
    let pending = Outcome()
    Task { @MainActor in
      _ = try? await entity.waitForInitialLoad()
      pending.finish()
    }

    await finishes("the confirmed logout") { try? await DB.sessionCall("logout", body: NoBody()) }
    try await DB.sessionReset()
    live.cancel()

    await eventually("the wait that was pending at the exit ended") { pending.isDone }
    #expect(entity.value == nil)
    server.hold(operation: nil)
    #expect(entity.value == nil, "the late answer of account A reached the held entity")
    try await Self.startSealedSession("B")
    await eventually(#"entity.value?.name == "B""#) { entity.value?.name == "B" }
  }

  /// C12: A, B, A with a held entity and a held list. After each exit the late answer of the previous account arrives and neither shows it.
  @MainActor
  @Test
  func anAccountSwitchABAKeepsAHeldListAndEntityFreeOfThePreviousAccount() async throws {
    let server = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    let history = RowHistory()
    let live = try DB.database.liveQuery(queryJson: #"{"model":"User"}"#, listener: history)
    defer { live.cancel() }
    await eventually("the held list shows A") { history.states.last?.contains("A") == true }
    for (next, previous) in [("B", "A"), ("A", "B")] {
      server.hold(operation: "UserProfile")
      Task { @MainActor in _ = try? await User.profile.refresh() }
      await eventually("the reread waits at the server") { server.parkedCount > 0 }
      await finishes("the confirmed logout") { try? await DB.sessionCall("logout", body: NoBody()) }
      try await DB.sessionReset()
      #expect(entity.value == nil)
      let seen = history.states.count
      server.hold(operation: nil)
      try await Self.startSealedSession(next)
      await eventually("the held entity shows \(next)") { entity.value?.name == next }
        await eventually("the held list shows \(next)") { history.states.last?.contains(next) == true }
      let after = history.states.dropFirst(seen)
      #expect(after.allSatisfy { !$0.contains(previous) }, "\(previous) came back after its exit: \(after)")
    }
  }

  /// Exit and login of the same account: the held entity empties and shows the account again, and the server sees a new read.
  @MainActor
  @Test
  func aLogoutThenALoginOfTheSameAccountReadsItAgain() async throws {
    let server = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    let reads = server.operationRequests("UserProfile")
    try await DB.sessionCall("logout", body: NoBody())
    try await DB.sessionReset()
    #expect(entity.value == nil)
    try await Self.startSealedSession("A")
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    #expect(server.operationRequests("UserProfile") > reads)
  }

  /// C09, R-7: the local-only exit sends no unregister and no logout. The held entity empties and no read follows.
  @MainActor
  @Test
  func aLocalOnlyExitSkipsTheRevokeAndStopsEveryRead() async throws {
    let server = try await Self.openSealedSession()
    try await Self.startSealedSession("A")
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    try await DB.sessionReset()
    #expect(entity.value == nil)
    let before = server.requests.count
    server.refuseEverything(true)
    #expect(server.requests.count == before)
    #expect(server.pathRequests("/api/logout") == 0)
    server.refuseEverything(false)
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
    try await DB.close()
    await eventually("the call ended without its answer") { ended.isDone }
    server.hold(operation: nil)
  }

  /// C19, R-5: the login is refused as already authenticated, the confirmed sign-out follows, the login repeats once and starts the session.
  @MainActor
  @Test
  func anAlreadyAuthenticatedLoginSignsOutThenLogsInAgain() async throws {
    let server = try await Self.openSealedSession()
    Self.alreadyAuthenticated.arm()
    var code: SessionCode?
    do { try await DB.sessionCall("login_link", body: LoginLink(account: "A")) } catch let error as DbError {
      if case let .Session(refused, _) = error { code = refused }
    }
    #expect(code == .alreadyAuthenticated)
    let logins = server.pathRequests("/api/auth/login_link")
    try await DB.sessionCall("logout", body: NoBody())
    try await Self.startSealedSession("A")
    #expect(server.pathRequests("/api/auth/login_link") == logins + 1)
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
  }
}
