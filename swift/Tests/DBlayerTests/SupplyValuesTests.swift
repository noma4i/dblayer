import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let singleOperations = #"[{"name":"CreateBlob","document":"mutation CreateBlob($filename: String!, $contentType: String!, $byteSize: Int!, $checksum: String!) { createBlob(filename: $filename, contentType: $contentType, byteSize: $byteSize, checksum: $checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#
private let operations = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]"#

extension BridgeProbeTests {
  /// The app order: a pending batch element, the file supplied while the server holds the blob request. The call returns and the row shows the supplied values at once.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func supplySourceReturnsAndShowsItsValuesWhileTheBlobRequestIsHeld() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("image".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: operations)
    let items = (0..<2).map { UploadItem(values: ["clientKey": .string("key-\($0)"), "size": .integer(2)], request: .pending()) }
    let receipt = try await DB.account().uploadBatch(Asset.self, items: items, request: UploadBatchRequest(operation: "create_files"))
    let held = transport.hold("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
    transport.enqueuePut("https://storage.test/file")
    let id = receipt.items[1].optimisticValue.id

    try await receipt.items[1].optimisticValue.supplySource(action: "create_files", source: UploadSource(path: source.path, values: ["size": .integer(3)]))

    let rows = try await Asset.ids([id]).read()
    #expect(rows.first?.size == 3, "the supplied value is in the row when the call returns")
    await held.entered()
    #expect(try await Asset.ids([id]).read().first?.size == 3, "the value is in the row when the blob request is sent")
    try await receipt.items[1].optimisticValue.discard(action: "create_files")
    try await receipt.items[0].optimisticValue.discard(action: "create_files")
    held.release()
  }

  /// The same order for a single send with a pending source (the order of the iOS app).
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func supplySourceOfASingleSendReturnsAndShowsItsValuesWhileTheBlobRequestIsHeld() async throws {
    let file = FileManager.default.temporaryDirectory.appending(path: "clip-\(UUID().uuidString).mp4")
    try Data("abcdef".utf8).write(to: file)
    defer { try? FileManager.default.removeItem(at: file) }
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: singleOperations)
    let held = transport.hold("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}"#)
    let sent = try await DB.account().send(Moment.self, values: ["text": .string("first")], upload: UploadCommand(action: "upload_clip", request: .pending(variables: ["text": .string("first")])))
    transport.enqueuePut("https://s3/x")
    let id = try #require(sent.optimisticValue).id

    try await sent.optimisticValue?.supplySource(action: "upload_clip", source: UploadSource(path: file.path, values: ["text": .string("supplied")]))

    #expect(try await Moment.ids([id]).read().first?.text == "supplied", "the supplied value is in the row when the call returns")
    await held.entered()
    #expect(try await Moment.ids([id]).read().first?.text == "supplied", "the value is in the row when the blob request is sent")
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    held.release()
    _ = try await sent.confirmed()
  }
}
