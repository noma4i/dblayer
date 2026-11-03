import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class HeldTestingTransport: Transport, @unchecked Sendable {
  let mock = MockTransport()
  private let lock = NSLock()
  private var waiter: CheckedContinuation<Void, Never>?
  var entered: Bool { lock.withLock { waiter != nil } }

  func release() { lock.withLock { let saved = waiter; waiter = nil; return saved }?.resume() }
  func query(request: TransportRequest) async throws -> String {
    await withCheckedContinuation { continuation in lock.withLock { waiter = continuation } }
    return try await mock.query(request: request)
  }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { try await mock.subscribe(request: request, sink: sink) }
  func unsubscribe(token: String) { mock.unsubscribe(token: token) }
  func openCable(lease: CableLease) async throws { try await mock.openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, tag: String, progress: ByteProgress) async throws -> PutReply { try await mock.put(url: url, headers: headers, file: file, tag: tag, progress: progress) }
}

private final class SwitchableTestTransport: Transport, @unchecked Sendable {
  private let lock = NSLock()
  private var target: MockTransport?

  init(_ target: MockTransport?) { self.target = target }

  func use(_ next: MockTransport?) { lock.withLock { target = next } }
  private func selected() throws -> MockTransport {
    guard let current = lock.withLock({ target }) else { throw DbError.Interrupted(detail: "transport unavailable during reset") }
    return current
  }
  func query(request: TransportRequest) async throws -> String { try await selected().query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await selected().mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { try await selected().subscribe(request: request, sink: sink) }
  func unsubscribe(token: String) { try? selected().unsubscribe(token: token) }
  func openCable(lease: CableLease) async throws { try await selected().openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await selected().cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await (try? selected())?.cableNext(token: token) }
  func cableClose(token: String) async { await (try? selected())?.cableClose(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, tag: String, progress: ByteProgress) async throws -> PutReply { try await selected().put(url: url, headers: headers, file: file, tag: tag, progress: progress) }
}

extension BridgeProbeTests {
  @MainActor @Test(.timeLimit(.minutes(1)))
  func testingResetGivesTheNextTransportAFreshOpen() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let reply = #"{"data":{"feed":{"nodes":[{"id":"m1","uuid":"first","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#
    let first = MockTransport()
    first.enqueue("MomentsFeed", reply: reply)
    let switched = SwitchableTestTransport(first)
    try await DB.openTesting(transport: switched, modelsJSON: modelsJSON, operations: [TestingFeed.self])
    let held = LiveList(Moment.feed)
    try await held.waitForInitialLoad()
    try DB.database.setOnline(online: false)
    switched.use(nil)
    try await DB.resetForTesting()
    try await DB.settleForTesting()
    let next = MockTransport()
    for _ in 0..<3 { next.enqueue("MomentsFeed", reply: reply) }
    switched.use(next)
    #expect(next.calls.isEmpty)
    let fresh = LiveList(Moment.feed)
    try await fresh.waitForInitialLoad()
    #expect(fresh.ids == ["m1"])
    #expect(!next.calls.isEmpty && next.calls.allSatisfy { $0.operationName == "MomentsFeed" })
    let entity = Moment.entity("m1")
    try await entity.waitForInitialLoad()
    #expect(entity.value != nil)
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func testingResetClearsRows() async throws {
    try await DB.close()
    let transport = MockTransport()
    let reply = #"{"data":{"feed":{"nodes":[{"id":"m1","uuid":"first","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#
    transport.enqueue("MomentsFeed", reply: reply)
    transport.enqueue("MomentsFeed", reply: reply)
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"id":"created","uuid":"u","createdAt":"2026-10-27T00:00:00Z"}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, operations: [TestingFeed.self, TestingCreate.self])
    defer { Task { @MainActor in try? await DB.close() } }
    try await Moment.feed.load()
    try DB.database.setOnline(online: false)
    let row = try await Moment.create(text: "before")
    let entity = Moment.entity(row.id)
    await eventually("the entity reads the row") { entity.value?.text == "before" }
    #expect(try await Moment.recent.live.current().count == 2)
    try await DB.resetForTesting()
    try await DB.settleForTesting()
    #expect(try await Moment.recent.live.current().isEmpty)
    #expect(entity.value == nil)
    try DB.database.setOnline(online: true)
    try await DB.settleForTesting()
    #expect(transport.calls.map(\.operationName) == ["MomentsFeed"])
    try await Moment.feed.load()
    #expect(transport.calls.map(\.operationName) == ["MomentsFeed", "MomentsFeed"])
  }

  @MainActor @Test(.timeLimit(.minutes(1)))
  func testingSettleWaitsForTheDeliveredLoad() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = HeldTestingTransport()
    transport.mock.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"id":"m1","uuid":"first","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, operations: [TestingFeed.self])
    let list = LiveList(Moment.feed)
    await eventually("the load starts") { transport.entered }
    transport.release()
    try await DB.settleForTesting()
    #expect(list.ids == ["m1"])
  }
}

private enum TestingFeed: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "MomentsFeed"
  static let document = "query MomentsFeed { feed { nodes { id uuid createdAt } pageInfo { hasNextPage endCursor } } }"
}

private enum TestingCreate: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "CreateMoment"
  static let document = "mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid createdAt } }"
}
