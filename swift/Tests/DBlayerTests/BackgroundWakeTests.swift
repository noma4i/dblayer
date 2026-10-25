import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class Calls: @unchecked Sendable {
  private let lock = NSLock()
  private var seen: [String] = []
  func add(_ name: String) { lock.withLock { seen.append(name) } }
  var all: [String] { lock.withLock { seen } }
}

extension BridgeProbeTests {
  @MainActor
  @Test
  func theWakeCompletionRunsAfterTheSessionEventsAndTheCoreSteps() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let source = directory.appending(path: "image.png")
    try Data("image".utf8).write(to: source)
    let transport = MockTransport()
    let database = try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 1), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size } } }"}]"#, uploads: UploadOperations(createBlob: UploadOperation("CreateBlob")))
    try database.setOnline(online: false)
    let items = (0..<2).map { UploadItem(values: ["clientKey": .string("key-\($0)"), "size": .integer(2)], path: source.path) }
    let rows = try await Asset.uploadCreate(items: items, request: UploadBatchRequest(operation: "create_files"))
    for _ in 0..<2 {
      transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#)
    }
    transport.enqueue("CreateAssets", reply: #"{"data":{"createAssets":{"assets":[{"id":"a1","clientKey":"key-1","size":2},{"id":"a0","clientKey":"key-0","size":2}]}}}"#)

    let scripted = ScriptedUploadTasks(identifier: "wake.test")
    let uploads = UrlSessionTransport(endpoint: URL(fileURLWithPath: "/unused"), uploads: scripted)
    uploads.connectUploads { database.putTasks(tasks: $0) }
    let wake = BackgroundWake()
    let calls = Calls()
    let (done, finish) = AsyncStream<Void>.makeStream()
    wake.handle(identifier: "wake.test") { calls.add("completion:\(transport.calls.filter { $0.operationName == "CreateAssets" }.count)"); finish.yield() }
    wake.handle(identifier: "other.session") { calls.add("other") }
    wake.attach(uploads: uploads, database: database)
    #expect(calls.all == ["other"])

    scripted.finish(tag: "1.0.1", etag: "e0")
    scripted.finish(tag: "1.1.1", etag: "e1")
    scripted.finishSessionEvents()
    try database.setOnline(online: true)
    _ = await Cursor(done).next("the wake completion")
    #expect(calls.all == ["other", "completion:1"])
    #expect(try await rows[0].confirmedId() == "a0")
  }
}
