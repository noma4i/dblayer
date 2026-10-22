import Foundation
import Testing
@testable import DBlayer

private let cloudModels = #"[{"key":"Note","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"},"optional":true}],"storage":"local","cloud":["title"]}]"#

private final class RecordingCloud: CloudIo, @unchecked Sendable {
  private let lock = NSLock()
  private var pushed: [CloudBlob] = []

  var blobs: [CloudBlob] { lock.withLock { pushed } }

  func account() async throws -> CloudAccount { .available }
  func pull(cursor: Data?, limit: UInt32) async throws -> CloudPage { CloudPage(blobs: [], cursor: Data([1]), more: false) }
  func push(blobs: [CloudBlob]) async throws { lock.withLock { pushed.append(contentsOf: blobs) } }
}

struct CloudBridgeTests {
  @Test
  func aLocalWriteReachesTheCloudAdapterThroughTheBridge() async throws {
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: cloudModels, transport: nil)
    let cloud = RecordingCloud()
    let link = try database.connectCloud(io: cloud)
    _ = try await database.create(model: "Note", valuesJson: #"{"title":"hello"}"#, argsJson: "{}")
    try await link.syncNow()
    #expect(cloud.blobs.count == 1)
    #expect(cloud.blobs.first.map { $0.key.count == 64 && !$0.bytes.isEmpty } == true)
    #expect(link.state() == .idle)
    link.stop()
  }
}
