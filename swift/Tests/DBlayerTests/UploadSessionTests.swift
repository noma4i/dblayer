import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class HeldUploadTransport: Transport, @unchecked Sendable {
  let base = MockTransport()
  let arrived = AsyncStream<Void>.makeStream()
  let released = AsyncStream<Void>.makeStream()
  let finished = AsyncStream<Void>.makeStream()

  func openCable(lease: CableLease) async throws { try await base.openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await base.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await base.cableNext(token: token) }
  func cableClose(token: String) async { await base.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await base.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { try await base.subscribe(request: request, sink: sink) }
  func unsubscribe(token: String) { base.unsubscribe(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, tag: String, progress: ByteProgress) async throws -> PutReply {
    defer { finished.continuation.yield(()) }
    arrived.continuation.yield(())
    _ = await Cursor(released.stream).next("the held upload is released")
    return PutReply(status: 200, etag: nil)
  }
}

private final class AcceptedUploads: UploadAcceptedSink, @unchecked Sendable {
  func accepted(event: WireUploadAccepted) {}
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func uploadRoundKeepsEachGroupsOriginalEpoch() async throws {
    let file = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("clip".utf8).write(to: file)
    defer { try? FileManager.default.removeItem(at: file) }
    let transport = MockTransport()
    transport.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}"#)
    let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#
    let database = try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: operations, uploads: UploadOperations(createBlob: UploadOperation("CreateBlob")))
    database.setUploadAcceptedSink(sink: AcceptedUploads())
    try database.setOnline(online: false)
    let first = database.sessionEpoch()
    let owner = try #require(try database.roundBegin(epoch: first))
    #expect(try database.roundNext(epoch: first, owner: owner) == .done)
    _ = try await DB.account().send(Moment.self, values: ["text": .string("A")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: file.path)))
    try await DB.sessionReset()
    do {
      _ = try database.roundNext(epoch: first, owner: owner)
      Issue.record("the old round call was accepted")
    } catch DbError.Interrupted {}
    try database.setOnline(online: true)
    try await User.profile.load(in: DB.account())
    try database.setOnline(online: false)
    let second = database.sessionEpoch()
    #expect(first != second)
    _ = try await DB.account().send(Moment.self, values: ["text": .string("B")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: file.path)))
    let groups = try #require(try database.roundEnd(epoch: second, owner: owner))
    #expect(groups.count == 2)
    #expect(groups.map(\.epoch) == [first, second])
    #expect(groups.map { $0.items.count } == [1, 1])
    try await DB.close()
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func resetDuringPutEndsTheWaitAndRemovesTheOldUpload() async throws {
    let file = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("clip".utf8).write(to: file)
    defer { try? FileManager.default.removeItem(at: file) }
    let transport = HeldUploadTransport()
    defer { transport.released.continuation.yield(()) }
    transport.base.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
    transport.base.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}"#)
    let database = try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#, uploads: UploadOperations(createBlob: UploadOperation("CreateBlob")))
    let epoch = database.sessionEpoch()
    let sent = try await DB.account().send(Moment.self, values: ["text": .string("hello")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: file.path)))
    let row = try #require(sent.optimisticValue)
    _ = try #require(await Cursor(transport.arrived.stream).next("the PUT starts"))
    let started = AsyncStream<Void>.makeStream()
    let result = AsyncStream<Bool>.makeStream()
    Task { @MainActor in
      started.continuation.yield(())
      do {
        if case .interrupted = try await sent.outcome() { result.continuation.yield(true) } else { result.continuation.yield(false) }
      } catch { result.continuation.yield(false) }
    }
    _ = try #require(await Cursor(started.stream).next("the wait starts"))
    try await DB.sessionReset()
    #expect(await Cursor(result.stream).next("the old upload wait ends") == true)
    do {
      try await database.resumeUploads(epoch: epoch)
      Issue.record("old upload resume was accepted")
    } catch DbError.Interrupted {}
    do {
      _ = try await database.upload(epoch: database.sessionEpoch(), model: "Moment", rowId: row.id, action: "upload_clip", request: UploadRequest(path: file.path).wire)
      Issue.record("upload accepted without a session")
    } catch DbError.NoSession {}
    try await User.profile.load(in: DB.account())
    #expect(try await User.profile.read(in: DB.account()).first?.name == "B")
    transport.released.continuation.yield(())
    _ = try #require(await Cursor(transport.finished.stream).next("the PUT ends"))
    let found = try await database.readRow(epoch: database.sessionEpoch(), model: "Moment", rowId: row.id)
    #expect(found == nil)
    #expect(transport.base.calls.filter { $0.operationName == "CreateMoment" }.isEmpty)
  }
}
