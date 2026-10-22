import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(arguments: [false, true])
  func batchDiscardRenumbersSnapshots(cancel: Bool) async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("file".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 1), modelsJson: modelsJSON, transport: MockTransport())
    try DB.database.setOnline(online: false)
    let items = (0..<4).map { UploadItem(values: ["clientKey": .string("key-\($0)"), "position": .number(Double($0)), "size": .number(4)], path: source.path) }
    let rows = try await Asset.uploadCreate(items: items, request: UploadBatchRequest(operation: "create_files", createBlob: "CreateBlob"))
    if cancel { #expect(try await rows[1].cancelUpload()) }
    else { try await rows[1].discard() }
    for (position, original) in [0, 2, 3].enumerated() {
      let snapshot = try #require(await Asset.find(rows[original].id).current())
      #expect(snapshot.position == position)
      #expect(snapshot.size == 3)
    }
  }

  @MainActor
  @Test(arguments: [false, true])
  func batchOwnsSourcesAndPreservesMembership(cancel: Bool) async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "image.png")
    try Data("image".utf8).write(to: source)
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.setOnline(online: false)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]"#)
    let items = (0..<2).map { UploadItem(values: ["clientKey": .string("key-\($0)"), "size": .number(2)], path: source.path) }
    let rows = try await Asset.uploadCreate(items: items, request: UploadBatchRequest(operation: "create_files", createBlob: "CreateBlob"))
    #expect(rows.count == 2)
    #expect(rows.allSatisfy { $0.id.hasPrefix("temp-") && $0.sync.state == .sending && $0.upload != nil })
    try FileManager.default.removeItem(at: source)
    if cancel {
      #expect(try await rows[0].cancelUpload())
      try await DB.database.warm(queryJson: #"{"model":"Asset"}"#)
      let first = try DB.database.peekRow(model: "Asset", rowId: rows[0].id)
      let second = try DB.database.peekRow(model: "Asset", rowId: rows[1].id)
      #expect(first != nil && first?.upload == nil)
      #expect(second?.upload != nil)
      #expect(transport.calls.isEmpty)
      return
    }
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
