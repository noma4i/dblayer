import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test
  func batchOwnsSourcesAndConfirmsEveryMemberWithOneRequest() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "image.png")
    try Data("image".utf8).write(to: source)
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 1), modelsJson: modelsJSON, transport: transport)
    try DB.database.setOnline(online: false)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]"#)
    let items = (0..<2).map { UploadItem(values: ["clientKey": .string("key-\($0)"), "size": .integer(2)], path: source.path) }
    let rows = try await Asset.uploadCreate(items: items, request: UploadBatchRequest(operation: "create_files", createBlob: "CreateBlob"))
    #expect(rows.count == 2)
    #expect(rows.allSatisfy { $0.id.hasPrefix("temp-") && $0.sync.state == .sending && $0.upload != nil })
    try FileManager.default.removeItem(at: source)
    for _ in 0..<2 {
      transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
    }
    transport.enqueue("CreateAssets", reply: #"{"data":{"createAssets":{"assets":[{"id":"a1","clientKey":"key-1","size":2},{"id":"a0","clientKey":"key-0","size":2}]}}}"#)
    try DB.database.setOnline(online: true)
    for (position, row) in rows.enumerated() { #expect(try await row.confirmedId() == "a\(position)") }
    let calls = transport.calls.filter { $0.operationName == "CreateAssets" }
    #expect(calls.count == 1)
    let variables = try JSONSerialization.jsonObject(with: Data(calls[0].variablesJson.utf8)) as! [String: Any]
    let sent = variables["items"] as! [[String: String]]
    #expect(sent.map { $0["clientKey"] } == ["key-0", "key-1"])
    #expect(sent.allSatisfy { $0["signedId"] == "signed" })
  }
}
