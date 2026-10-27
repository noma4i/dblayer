import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anUploadAnswersWithTheServerTextAndNilWhenTheServerLeftItOut() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("video".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let transport = MockTransport()
    let row = #""__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""#
    transport.enqueue("CreateMoment", reply: "{\"data\":{\"createMoment\":{\(row)}}}")
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]"#, uploads: UploadOperations(createBlob: UploadOperation("CreateBlob")))
    let local = try await Moment.create(text: "intro")
    try await local.settled(action: nil)
    let id = try await local.confirmedId()
    let moment = try #require(try await Moment.find(id).current())
    for expected in ["Intro received", nil] as [String?] {
      transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://storage.test/video","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
      let field = expected.map { ",\"successMessage\":\"\($0)\"" } ?? ""
      transport.enqueue("CreateMoment", reply: "{\"data\":{\"createMoment\":{\(row)\(field)}}}")
      let uploaded = try await moment.uploadClip(UploadRequest(path: source.path))
      #expect(uploaded.model.id == "m77")
      #expect(try await uploaded.answer().successMessage == expected)
    }
    #expect(transport.calls.map(\.operationName) == ["CreateMoment", "CreateBlob", "CreateMoment", "CreateBlob", "CreateMoment"])
    #expect(transport.unscripted.isEmpty)
  }
}
