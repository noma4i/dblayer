import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor
  func blobPlatforms() async throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let spec = try String(contentsOf: root.appendingPathComponent("fixtures/blob-upload.json"), encoding: .utf8)
    let document = try String(contentsOf: root.appendingPathComponent("fixtures/operations/queries/blob.graphql"), encoding: .utf8)
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: spec, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array(["BlobCreate", "BlobProfileCurrent", "BlobSetVideo"].map { .object(["name": .string($0), "document": .string(document), "rootPath": .string($0 == "BlobCreate" ? "blobCreate" : $0 == "BlobProfileCurrent" ? "blobProfile" : "setVideo")]) }).text)
    transport.enqueue("BlobProfileCurrent", reply: #"{"data":{"blobProfile":{"id":"p1","label":"before"}}}"#)
    transport.enqueue("BlobCreate", reply: #"{"data":{"blobCreate":{"url":"https://s3/blob","headers":{},"signedId":"signed-video","blobId":"b1"}}}"#)
    transport.enqueue("BlobSetVideo", reply: #"{"data":{"setVideo":{"profile":{"id":"p1","label":"after"}}}}"#)
    try await BlobProfile.current.load()
    let source = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".mp4")
    try Data("hello".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let blob = try await DB.upload(.videoIntro, request: UploadRequest(path: source.path))
    let restored = try await DB.blob(blob.id)
    #expect(restored.id == blob.id)
    #expect(try await blob.waitUntilUploaded() == "signed-video")
    await eventually("uploaded Blob snapshot") { blob.snapshot.state == .uploaded }
    #expect(blob.snapshot.signedId == "signed-video")
    #expect(blob.snapshot.progress == 1)
    #expect(transport.calls.filter { $0.operationName == "BlobSetVideo" }.isEmpty)
    _ = try await BlobProfile.setVideo(video: blob)
    await eventually("Blob consumer sent") { transport.calls.filter { $0.operationName == "BlobSetVideo" }.count == 1 }
    let call = try #require(transport.calls.first { $0.operationName == "BlobSetVideo" })
    let variables = try JSONDecoder().decode(JSONValue.self, from: Data(call.variablesJson.utf8))
    #expect(variables == .object(["blobProfileId": .string("p1"), "signedId": .string("signed-video")]))
    await eventually("consumed Blob snapshot") { blob.snapshot.state == .consumed || blob.snapshot.error != nil }
    #expect(blob.snapshot.state == .consumed)
    #expect(blob.snapshot.error == nil)
  }
}
