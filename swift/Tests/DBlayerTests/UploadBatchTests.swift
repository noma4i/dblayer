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
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 1), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]"#)
    try DB.database.setOnline(online: false)
    let items = (0..<2).map { UploadItem(values: ["clientKey": .string("key-\($0)"), "size": .integer(2)], path: source.path) }
    let receipt = try await DB.account().uploadBatch(Asset.self, items: items, request: UploadBatchRequest(operation: "create_files"))
    let rows = receipt.items.map(\.optimisticValue)
    #expect(rows.count == 2)
    #expect(receipt.items.map { $0.item.value } == ["key-0", "key-1"])
    #expect(rows.allSatisfy { $0.id.hasPrefix("temp-") && $0.sync.state == .sending && !$0.uploads.isEmpty })
    try FileManager.default.removeItem(at: source)
    for _ in 0..<2 {
      transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
    }
    transport.enqueue("CreateAssets", reply: #"{"data":{"createAssets":{"assets":[{"id":"a1","clientKey":"key-1","size":2},{"id":"a0","clientKey":"key-0","size":2}]}}}"#)
    try DB.database.setOnline(online: true)
    let waiting = Task { try await confirmedPairs(receipt) }
    let pairs = try await valueBeforeDeadline(waiting, release: {})
    let ids = pairs.compactMap { pair in pair.map { "\($0.key):\($0.id)" } }
    #expect(ids == ["key-0:a0", "key-1:a1"])
    let calls = transport.calls.filter { $0.operationName == "CreateAssets" }
    #expect(calls.count == 1)
    let variables = try JSONSerialization.jsonObject(with: Data(calls[0].variablesJson.utf8)) as! [String: Any]
    let sent = variables["items"] as! [[String: String]]
    #expect(sent.map { $0["clientKey"] } == ["key-0", "key-1"])
    #expect(sent.allSatisfy { $0["signedId"] == "signed" })
  }
}
