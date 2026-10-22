import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class UploadRows: QueryListener, @unchecked Sendable {
  private let lock = NSLock()
  private var latest: WireRow?
  var row: WireRow? { lock.withLock { latest } }
  func changed(change: LiveChange) {
    lock.withLock {
      if change.full || !change.rows.isEmpty { latest = change.rows.first }
    }
  }
}

struct UploadItemTests {
  @Test(arguments: [false, true])
  func ownedSingleFileSurvivesRestartOrCancellation(cancel: Bool) async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "clip.mp4")
    try Data("abcdef".utf8).write(to: source)
    let config = Config(path: directory.appending(path: "db.sqlite").path, dataVersion: nil, cacheRows: 32)
    let definitions = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"}]"#
    let transport = MockTransport()
    transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}"#)
    transport.enqueue("CreateMoment", failure: .Interrupted(detail: "lost answer"))
    var database: Database? = try await openDatabase(config: config, modelsJson: modelsJSON, transport: transport)
    defer { database = nil }
    try database!.registerOperations(defsJson: definitions)
    if cancel { try database!.setOnline(online: false) }
    let request = UploadRequest(path: source.path, mutation: "CreateMoment", createBlob: "CreateBlob")
    let local = try await database!.uploadCreate(model: "Moment", valuesJson: #"{"text":"clip"}"#, inputJson: request.json)
    let rows = UploadRows()
    let live = try database!.liveFind(model: "Moment", rowId: local.id, listener: rows)
    defer { live.cancel() }
    await eventually("the pending record is observed") { rows.row != nil }
    try FileManager.default.removeItem(at: source)
    if cancel {
      #expect(try await database!.cancelUpload(model: "Moment", rowId: local.id))
      await eventually("cancellation clears the observed upload") { rows.row != nil && rows.row?.upload == nil }
      #expect(rows.row?.id == local.id)
      #expect(transport.calls.isEmpty)
      return
    }
    await eventually("the transferred file keeps its unknown delivery") {
      rows.row?.upload?.status == .deliveryUnknown
    }
    let progress = try #require(rows.row?.upload)
    #expect(progress.sentBytes == 6)
    #expect(progress.totalBytes == 6)
    live.cancel()
    database = nil
    database = try await openDatabase(config: config, modelsJson: modelsJSON, transport: transport)
    try database!.registerOperations(defsJson: definitions)
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    _ = try await database!.retry(model: "Moment", rowId: local.id)
    #expect(try await database!.confirmed(model: "Moment", rowId: local.id) == "m77")
    #expect(transport.calls.filter { $0.operationName == "CreateBlob" }.count == 1)
    let final = transport.calls.filter { $0.operationName == "CreateMoment" }
    #expect(final.count == 2)
    #expect(final.allSatisfy { $0.variablesJson.contains("\"signedId\":\"sid\"") })
  }
}
