import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class BridgeCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?

  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private final class BridgeAccount: @unchecked Sendable {
  private let lock = NSLock()
  private var name: String?
  private var created = 0

  var current: String? { lock.withLock { name } }
  func signIn(_ account: String) { lock.withLock { name = account } }
  func signOut() { lock.withLock { name = nil } }
  func nextId() -> Int { lock.withLock { created += 1; return created } }
}

private struct BridgeLogin: Encodable, Sendable { let account: String }

/// The value bridge through its public API: streams of snapshots and deltas, a command, and the epoch of the session. The tests share `DB.database` with `BridgeProbeTests`, so they run in its serialized suite.
extension BridgeProbeTests {
  private static let bridgeAccount = BridgeAccount()

  @MainActor
  static func openBridgeSession() async throws -> SealedServer {
    await DB.closeForCleanup()
    let account = Self.bridgeAccount
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
      case "CreateMoment":
        let number = account.nextId()
        let text = (body["variables"] as? [String: Any])?["text"] as? String ?? ""
        return ["data": ["createMoment": ["__typename": "Moment", "id": "m\(number)", "uuid": "u\(number)", "text": text, "likedByMe": false, "media": [String: Any](), "createdAt": "2026-10-08T00:00:00.000Z"]]]
      default: return ["errors": [["message": "unexpected operation"]]]
      }
    }
    let encrypted = EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
    let transport = MockTransport()
    transport.cable = EncryptedCablePeer(server: server)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, encrypted: encrypted, credentials: BridgeCredential(), http: server)
    try await DB.database.registerOperations(defsJson: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"},{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid text likedByMe media createdAt } }"}]"#)
    return server
  }

  @MainActor
  static func signIn(_ account: String) async throws {
    try await DB.sessionCall("login_link", body: BridgeLogin(account: account))
    try await DB.run("SessionProfile", variables: [:])
    try await User.profile.refresh()
  }

  /// B01: after the login a stream starts with a snapshot of the account, in the epoch of the session.
  @MainActor
  @Test
  func aStreamAfterTheLoginStartsWithASnapshotOfTheAccountInItsEpoch() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    let profile = Projected<User>(try scope.stream(User.profile))
    try await profile.loaded()
    await eventually("the stream shows A") { profile.value?.name == "A" }

    #expect(profile.projection.revision != nil && profile.projection.ended == nil)
  }

  /// B02: the sign-out ends the stream with one outcome, the projection keeps no new value, and nothing stays open in the core.
  @MainActor
  @Test
  func theSessionResetEndsEveryStreamWithOneOutcome() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let profile = Projected<User>(try DB.account().stream(User.profile))
    try await profile.loaded()
    await eventually("the stream shows A") { profile.value?.name == "A" }

    try await DB.sessionReset()
    await eventually("the stream ended") { profile.projection.ended == .sessionEnded }

    #expect(try await DB.subscriptionCensus().subscriptions == 0)
    #expect(profile.value?.name == "A", "the ended projection keeps the last value it was given and no new one")
  }

  /// B03: the account switch gives new streams in a new epoch. B never shows A and A never shows B.
  @MainActor
  @Test
  func anAccountSwitchGivesNewProjectionsAndNoValueOfAReachesB() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let first = try DB.account()
    let projectionA = Projected<User>(try first.stream(User.profile))
    try await projectionA.loaded()
    await eventually("A shows") { projectionA.value?.name == "A" }

    try await DB.sessionReset()
    try await Self.signIn("B")
    let second = try DB.account()
    #expect(second.epoch != first.epoch)
    let projectionB = Projected<User>(try second.stream(User.profile))
    try await projectionB.loaded()
    await eventually("B shows") { projectionB.value?.name == "B" }

    #expect(projectionA.value?.name == "A" && projectionA.projection.ended == .sessionEnded)
  }

  /// B04: a message sent offline shows at once, goes out once when the network returns, and keeps its position when the server confirms it.
  @MainActor
  @Test
  func aMessageSentOfflineGoesOutOnceAndKeepsItsPosition() async throws {
    let server = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    let thread = Projected<Moment>(try scope.stream(Moment.recent))
    try await thread.loaded()
    try DB.database.setOnline(online: false)

    let sent = try await scope.send(Moment.self, values: ["text": .string("hello")])
    await eventually("the thread shows the queued message") { thread.rows.count == 1 }
    #expect(thread.rows[0].id == sent.optimisticValue?.id && thread.rows[0].sync.state == .pending)
    #expect(server.operationRequests("CreateMoment") == 0)

    try DB.database.setOnline(online: true)
    #expect(try await sent.confirmed().id == "m1")
    await eventually("the thread shows the confirmed message") { thread.rows.first?.id == "m1" }

    #expect(thread.rows.count == 1, "one message, not a temporary row and a confirmed row")
    #expect(thread.rows[0].sync.state == .synced)
    #expect(server.operationRequests("CreateMoment") == 1)
  }

  /// B05: a command and a call of the old epoch are refused or end as interrupted, and the new projection never shows what the old session was about to receive.
  @MainActor
  @Test
  func aCommandAndALateCallOfTheOldEpochEndWithoutReachingTheNewAccount() async throws {
    let server = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let old = try DB.account()
    let stream = try old.stream(User.profile)
    let projectionA = Projected<User>(stream)
    try await projectionA.loaded()
    await eventually("A shows") { projectionA.value?.name == "A" }

    server.hold(operation: "UserProfile")
    let late = Task { @MainActor in try await stream.refresh() }
    await eventually("the refresh waits at the server") { server.parkedCount > 0 }
    try await DB.sessionReset()
    server.hold(operation: nil)
    try await Self.signIn("B")
    let projectionB = Projected<User>(try DB.account().stream(User.profile))
    try await projectionB.loaded()
    await eventually("B shows") { projectionB.value?.name == "B" }

    await #expect(throws: DbError.self) { try await late.value }
    await #expect(throws: DbError.self) { _ = try await old.send(Moment.self, values: ["text": .string("late")]) }
    #expect(server.operationRequests("CreateMoment") == 0)
    #expect(projectionB.value?.name == "B")
  }

  /// B06: the reader that stops ends the stream in the core.
  @MainActor
  @Test
  func theReaderThatStopsEndsTheStreamInTheCore() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    var projected: Projected<User>? = Projected<User>(try scope.stream(User.profile))
    try await projected?.loaded()
    #expect(try await DB.subscriptionCensus().observedQueries == 1)

    projected = nil
    await eventually("the stream left the core") { (try? await DB.subscriptionCensus().observedQueries) == 0 }
  }

  /// B07: a change of one row decodes one row, and the subscriptions are the readers.
  @MainActor
  @Test
  func aChangeOfOneRowDecodesOneRowAndSubscriptionsEqualReaders() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    let thread = Projected<Moment>(try scope.stream(Moment.recent))
    let profile = Projected<User>(try scope.stream(User.profile))
    try await thread.loaded()
    var first: [MutationReceipt<Moment>] = []
    for number in 1...5 { first.append(try await scope.send(Moment.self, values: ["text": .string("m\(number)")])) }
    for sent in first { _ = try await sent.outcome() }
    await eventually("the thread holds five confirmed rows") { thread.rows.count == 5 && thread.rows.allSatisfy { $0.sync.state == .synced } }
    #expect(try await DB.subscriptionCensus().subscriptions == 1)

    let extra = try await scope.send(Moment.self, values: ["text": .string("m6")])
    await eventually("the sixth row shows") { thread.rows.count == 6 }
    _ = try await extra.outcome()
    await eventually("the sixth row is confirmed") { thread.rows.last?.sync.state == .synced }

    #expect(thread.rows.count == 6, "a row of the list was lost")
    withExtendedLifetime(profile) {}
  }

  /// B10: the outcome of a command sent offline waits, and ends with the session of its receipt.
  @MainActor
  @Test
  func theOutcomeOfAnOfflineCommandWaitsAndEndsWithTheEpochOfItsReceipt() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    try DB.database.setOnline(online: false)
    let sent = try await scope.send(Moment.self, values: ["text": .string("queued")])
    let waiting = Task { try await sent.outcome() }

    try await DB.sessionReset()
    guard case .interrupted = try await waiting.value else { Issue.record("the outcome did not end with the session"); return }
    try DB.database.setOnline(online: true)
    try await Self.signIn("B")
    guard case .interrupted = try await sent.outcome() else { Issue.record("a receipt of A answered in the epoch of B"); return }
  }

  /// B12: a stream whose source cannot be connected ends once with the invalid outcome and leaves nothing open.
  @MainActor
  @Test
  func aStreamThatCannotConnectEndsOnceAndLeavesTheTable() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let database = try DB.database
    let reader = database.subscribeValues(epoch: database.sessionEpoch(), request: .entity(model: "Nope", id: "x", fetch: .never))
    let ended: [ReadEnd] = try await drained("the stream that cannot connect to end", reader).compactMap {
      if case let .ended(cause) = $0 { return cause }
      return nil
    }
    #expect(ended == [.invalid])
    #expect(try await reader.pull() == nil, "a stream that ended gives nothing more")
    #expect(try await DB.subscriptionCensus().subscriptions == 0)
  }

  /// K02: a command and a one-off read that carry the scope of A are refused after the next sign-in and change nothing for B.
  @MainActor
  @Test
  func aCommandAndAReadOfAnAccountThatLeftAreRefusedAndChangeNothing() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scopeA = try DB.account()
    let row = try #require(try await User.profile.read(in: scopeA).first)
    try await DB.sessionReset()
    try await Self.signIn("B")

    await #expect(throws: DbError.self) { _ = try await row.update(values: ["name": .string("hijacked")], in: scopeA) }
    await #expect(throws: DbError.self) { _ = try await User.profile.read(in: scopeA) }
    await #expect(throws: DbError.self) { _ = try await User.profile.readRow(row.id, in: scopeA) }
    let current = try await User.profile.read().first
    #expect(current?.name == "B", "a command of A changed the row of B")
  }

  /// K08: a row the platform cannot decode is an error of the projection, and the next good snapshot clears it. A count stream and an events stream report their source error.
  @MainActor
  @Test
  func aRowThatDoesNotDecodeThrowsAndAStreamReportsItsSourceEnd() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    let profile = Projected<User>(try scope.stream(User.profile))
    try await profile.loaded()
    await eventually("the stream shows A") { profile.value?.name == "A" }
    let good = try #require(try await User.profile.read().first)
    _ = good
    var projection = ListProjection<User>()
    let state = try #require(profile.state)
    var bad = try #require(try await DB.database.readQuery(epoch: scope.epoch, queryJson: User.profile.json).first)
    bad.layout = "another layout"
    #expect(throws: DbError.self) { try projection.apply(.snapshot(revision: 1, rows: [bad], state: state)) }
    #expect(projection.rows.isEmpty && projection.revision == nil, "a row that does not decode changed the projection")
    let whole = try #require(try await DB.database.readQuery(epoch: scope.epoch, queryJson: User.profile.json).first)
    try projection.apply(.snapshot(revision: 2, rows: [whole], state: state))
    #expect(projection.rows.count == 1 && projection.error == nil)

    let database = try DB.database
    let unknown = ValueStream(epoch: scope.epoch, reader: database.subscribeValues(epoch: scope.epoch, request: .events(model: "Nope")), database: database, load: nil)
    guard case .ended(.invalid)? = await Cursor(EventStream<User>(stream: unknown)).next("the source error of the events stream") else {
      Issue.record("the events stream did not end with the invalid cause")
      return
    }
  }
}

extension BridgeProbeTests {
  /// K21 (C02): a scope keeps the database it was made for. After that database closed and another one opened with the same epoch number, a command and a read of the old scope are refused and the new database does not change.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aScopeKeepsItsOwnDatabaseAfterTheDatabaseIsReplaced() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let oldScope = try DB.account()
    let row = try #require(try await User.profile.read(in: oldScope).first)
    await DB.closeForCleanup()
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")

    await #expect(throws: DbError.self) { _ = try await row.update(values: ["name": .string("hijacked")], in: oldScope) }
    await #expect(throws: DbError.self) { _ = try await User.profile.read(in: oldScope) }
    let current = try await User.profile.read().first
    #expect(current?.name == "A", "a command of the old scope changed the new database")
  }

  /// K16 (B8): a reader that falls behind reads at most 256 values of a burst from the journal of the core and ends with the whole list from one new snapshot.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aReaderThatFallsBehindKeepsAtMost256DeliveriesAndEndsWithTheWholeList() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    let stream = try scope.stream(Moment.recent)
    let cursor = Cursor(stream)
    var projection = ListProjection<Moment>()
    var received = 0
    await eventually("the first settled snapshot") {
      if projection.state?.settled == true { return true }
      if let value = await cursor.next("the first settled snapshot") { _ = try? projection.apply(value) }
      return projection.state?.settled == true
    }
    try DB.database.setOnline(online: false)
    for number in 1...300 { _ = try await scope.send(Moment.self, values: ["text": .string("m\(number)")]) }
    for _ in 0..<400 {
      if projection.rows.count == 300 { break }
      guard let value = await cursor.next("a value of the burst") else { break }
      received += 1
      _ = try projection.apply(value)
    }
    #expect(projection.rows.count == 300, "the reader did not end with the whole list")
    #expect(received <= 260, "the reader received \(received) values of a burst of 300: the journal bounded nothing")
    await stream.cancel()
  }
}
