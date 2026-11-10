import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  /// D-287: the call on the type uploads to a row that does not exist: the receipt shows no optimistic value, the queue shows the transfer by its key, and the answer of the final mutation lands the row and reaches `confirmed()`.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func upload_to_a_missing_row_through_the_call_on_the_type() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("video".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let transport = MockTransport()
    let row = #""__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""#
    let database = try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]"#)
    try database.setOnline(online: false)
    let sent = try await Moment.uploadClip(id: "absent", source: source.path, in: DB.account())
    #expect(sent.optimisticValue == nil)
    var shown: UploadInfo?
    let queue = Cursor(try await DB.account().operations())
    if case let .queue(_, items)? = await queue.next("the queue") { shown = items.first?.uploadInfo }
    #expect((shown.map { [.waiting, .queued].contains($0.status) } ?? false) && shown?.totalBytes == 5, "the queue shows the transfer before it ends: \(String(describing: shown))")
    transport.enqueue("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
    transport.enqueuePut("https://storage.test/video")
    transport.enqueue("CreateMoment", reply: "{\"data\":{\"createMoment\":{\(row),\"successMessage\":\"Intro received\"}}}")
    try database.setOnline(online: true)
    let confirmed = try await sent.confirmed()
    #expect(confirmed.value?.id == "m77", "the answer landed the row")
    #expect(try await sent.answer(as: MomentUploadClipResult.self).successMessage == "Intro received")
    #expect(transport.calls.map(\.operationName) == ["CreateBlob", "CreateMoment"])
    #expect(transport.unscripted.isEmpty)
    await DB.closeForCleanup()
  }
}
