import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func inv_upload_21_an_upload_answers_with_the_server_text_and_nil_when_the_server_left_it_out() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("video".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let transport = MockTransport()
    let row = #""__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""#
    transport.enqueue("CreateMoment", reply: "{\"data\":{\"createMoment\":{\(row)}}}")
    try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]"#)
    let id = try #require(await Moment.create(text: "intro", in: DB.account()).confirmed().value).id
    let moment = try #require(try await Moment.find(id).current())
    for (turn, expected) in (["Intro received", nil] as [String?]).enumerated() {
      if turn == 0 { transport.enqueue("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}"#) }
      if turn == 0 { transport.enqueuePut("https://storage.test/video") }
      let field = expected.map { ",\"successMessage\":\"\($0)\"" } ?? ""
      transport.enqueue("CreateMoment", reply: "{\"data\":{\"createMoment\":{\(row)\(field)}}}")
      let uploaded = try await moment.uploadClip(UploadRequest(path: source.path), in: DB.account())
      #expect(uploaded.optimisticValue?.id == "m77")
      #expect(try await uploaded.answer(as: MomentUploadClipResult.self).successMessage == expected)
    }
    #expect(transport.calls.map(\.operationName) == ["CreateMoment", "CreateBlob", "CreateMoment", "CreateMoment"])
    #expect(transport.unscripted.isEmpty)
  }
}
