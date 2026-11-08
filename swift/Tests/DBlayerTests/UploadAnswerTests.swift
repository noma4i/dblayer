import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func inv_upload_14_a_waiting_upload_reports_the_copied_file_size() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("video".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let transport = MockTransport()
    let database = try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id } }"}]"#)
    try database.setOnline(online: false)
    let local = try await Moment.create(text: "intro", in: DB.account()).optimisticValue!
    _ = try await local.uploadClip(UploadRequest(path: source.path), in: DB.account())
    let current = try #require(try await Moment.find(local.id).current())
    let waiting = try #require(current.uploads["upload_clip"])
    #expect(waiting.status == .waiting)
    #expect(waiting.totalBytes == 5 && waiting.sentBytes == 0 && waiting.progress == 0)
    #expect(transport.calls.isEmpty)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func inv_upload_21_an_upload_answers_with_the_server_text_and_nil_when_the_server_left_it_out() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("video".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let transport = MockTransport()
    let row = #""__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""#
    transport.enqueue("CreateMoment", reply: "{\"data\":{\"createMoment\":{\(row)}}}")
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]"#)
    let id = try await Moment.create(text: "intro", in: DB.account()).confirmed().id
    let moment = try #require(try await Moment.find(id).current())
    for (turn, expected) in (["Intro received", nil] as [String?]).enumerated() {
      if turn == 0 { transport.enqueue("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}"#) }
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
