import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class RecordingPutTransport: ScriptedTransport, WrapsMock, @unchecked Sendable {
  let base = MockTransport()
  private let lock = NSLock()
  private var recorded: [Data] = []
  var bodies: [Data] { lock.withLock { recorded } }

  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome { await base.scriptedCable(url: url, events: events) }
  func query(request: TransportRequest) async throws -> String { try await base.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    let body = try Data(contentsOf: URL(fileURLWithPath: file.path))
    lock.withLock { recorded.append(body) }
    return try await base.scriptedPut(url: url, headers: headers, file: file, identity: identity, progress: progress)
  }
}

private let uploadOperations = #"[{"name":"CreateBlob","document":"mutation CreateBlob($filename: String!, $contentType: String!, $byteSize: Int!, $checksum: String!) { createBlob(filename: $filename, contentType: $contentType, byteSize: $byteSize, checksum: $checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#

private func enqueueUpload(_ transport: RecordingPutTransport) {
  transport.base.enqueue("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}"#)
  transport.base.enqueuePut("https://s3/x")
  transport.base.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
}

private func source(_ text: String) throws -> URL {
  let file = FileManager.default.temporaryDirectory.appending(path: "clip-\(UUID().uuidString)")
  try Data(text.utf8).write(to: file)
  return file
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aSourceIsSentAsIsAndRefusedAboveItsLimit() async throws {
    let transport = RecordingPutTransport()
    enqueueUpload(transport)
    _ = try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: uploadOperations)
    let file = try source("abcdef")
    defer { try? FileManager.default.removeItem(at: file) }
    let sent = try await DB.account().send(Moment.self, values: ["text": .string("hello")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: file.path, prepare: .image(edit: PhotoEdit(maxSize: nil, enlarge: false, crop: nil, outputSize: nil, mirror: false, rotationDegrees: 0)))))
    #expect(try await sent.confirmed().value?.id == "m77")
    #expect(transport.bodies == [Data("abcdef".utf8)])
    let refused = try await DB.account().send(Moment.self, values: ["text": .string("large")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: file.path, maxBytes: 5)))
    let id = try #require(refused.optimisticValue).id
    _ = try await refused.settled()
    #expect(try await Moment.find(id).current()?.uploads["upload_clip"]?.error?.kind == "too_large")
    #expect(transport.bodies == [Data("abcdef".utf8)])
    await DB.closeForCleanup()
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aDeviceThatCannotDecodeShowsTheReasonOfTheFailure() async throws {
    let transport = RecordingPutTransport()
    enqueueUpload(transport)
    _ = try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: uploadOperations, preparer: ScriptedPreparer(outcome: .cannotDecode(message: "no decoder")))
    let file = try source("abcdef")
    defer { try? FileManager.default.removeItem(at: file) }
    let sent = try await DB.account().send(Moment.self, values: ["text": .string("hello")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: file.path, prepare: .video(mirror: true))))
    let id = try #require(sent.optimisticValue).id
    _ = try await sent.settled()
    let failed = try #require(try await Moment.find(id).current()?.uploads["upload_clip"]?.error)
    #expect(failed.kind == "prepare_failed")
    #expect(failed.reason == "device_cannot_decode")
    #expect(transport.bodies.isEmpty)
    await DB.closeForCleanup()
  }
}
