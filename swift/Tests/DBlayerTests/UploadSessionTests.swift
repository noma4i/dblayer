import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class HeldUploadTransport: ScriptedTransport, @unchecked Sendable {
  let base = MockTransport()
  let arrived = AsyncStream<Void>.makeStream()
  let finished = AsyncStream<Void>.makeStream()
  private let lock = NSLock()
  private var putCount = 0
  var puts: Int { lock.withLock { putCount } }

  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome { await base.scriptedCable(url: url, events: events) }
  func query(request: TransportRequest) async throws -> String { try await base.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    lock.withLock { putCount += 1 }
    defer { finished.continuation.yield(()) }
    arrived.continuation.yield(())
    try await parkUntilCancelled()
    return PutReply(status: 200, etag: nil)
  }
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func resetDuringPutEndsTheWaitAndCancelsThePut() async throws {
    let file = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("clip".utf8).write(to: file)
    defer { try? FileManager.default.removeItem(at: file) }
    let transport = HeldUploadTransport()
    transport.base.enqueue("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
    _ = try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"}]"#)
    let sent = try await DB.account().send(Moment.self, values: ["text": .string("hello")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: file.path)))
    _ = try #require(await Cursor(transport.arrived.stream).next("the PUT starts"))
    let started = AsyncStream<Void>.makeStream()
    let result = AsyncStream<Bool>.makeStream()
    Task { @MainActor in
      started.continuation.yield(())
      do {
        if case .ended(cause: .sessionEnded) = try await sent.settled() { result.continuation.yield(true) } else { result.continuation.yield(false) }
      } catch { result.continuation.yield(false) }
    }
    _ = try #require(await Cursor(started.stream).next("the wait starts"))
    try await DB.sessionReset()
    #expect(await Cursor(result.stream).next("the old upload wait ends") == true)
    _ = try #require(await Cursor(transport.finished.stream).next("the cancelled PUT ends"))
    #expect(transport.puts == 1)
    await DB.closeForCleanup()
  }
}
