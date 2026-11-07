import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class PageTransport: Transport, @unchecked Sendable {
  let mock = MockTransport()
  private let lock = NSLock()
  private var requests = 0
  private let cancelAt: Int
  private let cancelled: @Sendable () -> Void

  init(cancelAt: Int = 6, cancelled: @escaping @Sendable () -> Void) { self.cancelAt = cancelAt; self.cancelled = cancelled }

  func query(request: TransportRequest) async throws -> String {
    if request.operationName != "ContainerMembers" { return try await mock.query(request: request) }
    let watched = lock.withLock { requests += 1; return requests == cancelAt }
    if !watched { return try await mock.query(request: request) }
    return try await withTaskCancellationHandler {
      try await self.mock.query(request: request)
    } onCancel: { self.cancelled() }
  }

  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func openCable(lease: CableLease, url: String) async throws { try await mock.openCable(lease: lease, url: url) }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    try await mock.put(url: url, headers: headers, file: file, identity: identity, progress: progress)
  }
}

extension BridgeProbeTests {
  private static let membersDocument = "query ContainerMembers($containerId: ID!, $first: Int, $after: Cursor) { node(id: $containerId) { ... on Container { membersCount members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

  private func membersPage(_ id: String, hasNext: Bool) -> String {
    #"{"data":{"node":{"membersCount":53,"members":{"edges":[{"cursor":"\#(id)","node":{"id":"\#(id)","name":"Member"}}],"pageInfo":{"hasNextPage":\#(hasNext),"endCursor":"\#(id)"}}}}}"#
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func cancellingTheLastLoadInterestCancelsTheRequest() async throws {
    let (signals, signal) = AsyncStream<Int>.makeStream()
    let transport = PageTransport(cancelAt: 1) { signal.yield(2) }
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    let hold = transport.mock.hold("ContainerMembers", reply: membersPage("a0", hasNext: false))
    defer { hold.release(); signal.finish() }
    let request = try User.containerMembers("c1").beginLoad()
    defer { request.cancel() }
    let entered = Task { await hold.entered(); signal.yield(1) }
    defer { entered.cancel() }
    let cursor = Cursor(signals)
    guard await cursor.next("the request starts") == 1 else { Issue.record("the request did not start"); return }
    request.cancel()
    guard await cursor.next("the transport cancels") == 2 else { Issue.record("the request outlived its last interest"); return }
    #expect(transport.mock.calls.filter { $0.operationName == "ContainerMembers" }.count == 1)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func cancellingTheCallerOfLoadAllStopsTheTransportCalls() async throws {
    let (signals, signal) = AsyncStream<Int>.makeStream()
    let transport = PageTransport { signal.yield(3) }
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    for number in 0..<5 { transport.mock.enqueue("ContainerMembers", reply: membersPage("a\(number)", hasNext: true)) }
    let sixth = transport.mock.hold("ContainerMembers", reply: membersPage("a5", hasNext: true))
    defer { sixth.release(); signal.finish() }
    let query = try User.containerMembers("c1")
    let loading = Task { try await query.loadAll() }
    let entered = Task { await sixth.entered(); signal.yield(1) }
    defer { entered.cancel(); loading.cancel() }
    let cursor = Cursor(signals)
    guard await cursor.next("the sixth request starts", timeout: .seconds(20)) == 1 else { return }
    loading.cancel()
    let finished = Task { _ = await loading.result; signal.yield(2) }
    defer { finished.cancel() }
    guard let first = await cursor.next("the cancelled request ends", timeout: .seconds(20)),
          let second = await cursor.next("the caller ends", timeout: .seconds(20)) else { return }
    #expect(Set([first, second]) == Set([2, 3]))
    #expect(transport.mock.calls.filter { $0.operationName == "ContainerMembers" }.count == 6)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func containerMembersPassesArgumentsAndDecodesTypedSnapshots() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    transport.enqueue("ContainerMembers", reply: membersPage("a0", hasNext: true))
    transport.enqueue("ContainerMembers", reply: membersPage("a1", hasNext: false))

    let first = try User.containerMembers("c1")
    try await first.load()
    let held = Projected<User>(try DB.account().stream(first))
    await eventually("the member count") { held.state?.total == 53 }
    #expect(held.state?.total == 53)
    #expect(held.rows.map(\.id) == ["a0"])
    #expect(held.rows.first?.name == "Member")
    #expect(held.state?.hasMore == true)
    #expect(try await first.loadMore() == .loaded(hasMore: false))
    let calls = transport.calls.filter { $0.operationName == "ContainerMembers" }
    let variables = try calls.map { try JSONDecoder().decode(JSONValue.self, from: Data($0.variablesJson.utf8)) }
    #expect(variables == [
      .object(["containerId": .string("c1"), "first": .integer(30)]),
      .object(["containerId": .string("c1"), "first": .integer(30), "after": .string("a0")]),
    ])
  }

  @Test
  func loadAllReadsEveryPageAndForceRestartsTheList() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    transport.enqueue("ContainerMembers", reply: membersPage("a0", hasNext: true))
    transport.enqueue("ContainerMembers", reply: membersPage("a1", hasNext: false))
    transport.enqueue("ContainerMembers", reply: membersPage("a2", hasNext: false))

    let members = try User.containerMembers("c1")
    try await members.loadAll()
    try await members.loadAll(force: true)
    let calls = transport.calls.filter { $0.operationName == "ContainerMembers" }
    let variables = try calls.map { try JSONDecoder().decode(JSONValue.self, from: Data($0.variablesJson.utf8)) }
    #expect(variables == [
      .object(["containerId": .string("c1"), "first": .integer(30)]),
      .object(["containerId": .string("c1"), "first": .integer(30), "after": .string("a0")]),
      .object(["containerId": .string("c1"), "first": .integer(30)]),
    ])
  }

  @Test @MainActor
  func membersSnapshotPlatforms() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    transport.enqueue("ContainerMembers", reply: membersPage("a0", hasNext: true))
    transport.enqueue("ContainerMembers", reply: membersPage("a1", hasNext: false))
    let query = try User.completeMembers("c1")
    let members = ListReader(query)
    await eventually("complete members") { members.snapshot.complete }
    #expect(members.snapshot.phase == .loaded)
    #expect(members.snapshot.rows.map(\.id) == ["a0", "a1"])
    #expect(members.snapshot.error == nil)
    #expect(members.snapshot.rows.map(\.id) == members.rows.map(\.id))
    transport.enqueue("ContainerMembers", reply: #"{"errors":[{"message":"refused"}]}"#)
    do { try await query.refresh(); Issue.record("a refused refresh must throw") } catch {}
    await eventually("members error") { members.snapshot.error != nil }
    #expect(members.snapshot.phase == .loaded)
    #expect(!members.snapshot.complete)
    #expect(members.snapshot.rows.map(\.id) == ["a0", "a1"])
    #expect(members.snapshot.error != nil)
  }
}
