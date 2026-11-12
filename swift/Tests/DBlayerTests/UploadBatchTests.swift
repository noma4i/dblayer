import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test
  func pendingBatchAcceptsAndSuppliesSources() async throws {
    let source = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("image".utf8).write(to: source)
    defer { try? FileManager.default.removeItem(at: source) }
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]"#)
    try DB.database.setOnline(online: false)
    let items = (0..<2).map { UploadItem(values: ["clientKey": .string("key-\($0)"), "size": .integer(2)], request: .pending()) }
    let receipt = try await DB.account().uploadBatch(Asset.self, items: items, request: UploadBatchRequest(operation: "create_files"))
    #expect(receipt.items.allSatisfy { $0.optimisticValue.uploads["create_files"]?.waitReason == "source_pending" })
    try await receipt.items[1].optimisticValue.supplySource(action: "create_files", source: UploadSource(path: source.path, values: ["size": .integer(3)]))
    try await receipt.items[0].optimisticValue.failSource(action: "create_files", message: "preparation")
    let retry = try await receipt.items[0].optimisticValue.retry(action: "create_files", source: UploadSource(path: source.path))
    #expect(retry.optimisticValue != nil)
    #expect(transport.calls.isEmpty)
  }
}
