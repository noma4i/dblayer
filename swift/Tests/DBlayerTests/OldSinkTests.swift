import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class OldSinkTransport: Transport, @unchecked Sendable {
  let base = MockTransport()
  let opened = AsyncStream<SubscriptionSink>.makeStream()
  let closed = AsyncStream<String>.makeStream()
  func openCable(lease: CableLease, url: String) async throws { try await base.openCable(lease: lease, url: url) }
  func cableSend(token: String, text: String) async throws { try await base.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await base.cableNext(token: token) }
  func cableClose(token: String) async { await base.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await base.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws {
    try await base.subscribe(request: request, sink: sink)
    opened.continuation.yield(sink)
  }
  func unsubscribe(token: String) { base.unsubscribe(token: token); closed.continuation.yield(token) }
  func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply { try await base.put(url: url, headers: headers, file: file, identity: identity, progress: progress) }
}

private final class IgnoredSink: ValueSink {
  func available() {}
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func oldSubscriptionSinkIsClosedBeforeTheNextAccount() async throws {
    try await DB.close()
    let transport = OldSinkTransport()
    transport.base.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    let stream = try DB.database.subscribe(epoch: DB.database.sessionEpoch(), request: .query(queryJson: #"{"model":"Notice","relation":"recent","params":{}}"#), sink: IgnoredSink())
    let old = try #require(await Cursor(transport.opened.stream).next("the subscription opens"))
    try await DB.sessionReset()
    #expect(await Cursor(transport.closed.stream).next("the subscription closes") == "BulletinAdded")
    try await User.profile.load(in: DB.account())
    try old.event(json: #"{"data":{"bulletinAdded":{"body":"old","occurredAt":"2026-10-18T00:00:00Z"}}}"#)
    let rows = try await DB.database.readQuery(epoch: DB.database.sessionEpoch(), queryJson: #"{"model":"Notice","relation":"recent","params":{}}"#)
    #expect(rows.isEmpty)
    try DB.database.cancelStream(stream: stream)
  }
}
