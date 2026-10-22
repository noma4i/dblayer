import Foundation
import Testing
@testable import DBlayer

private let cloudModels = #"[{"key":"Note","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"},"optional":true}],"storage":"local","cloud":["title"]}]"#

private final class RecordingCloud: CloudIo, @unchecked Sendable {
  private let lock = NSLock()
  private var pushed: [CloudChange] = []

  var changes: [CloudChange] { lock.withLock { pushed } }

  func account() async throws -> CloudAccount { .available }
  func pull(cursor: Data?, limit: UInt32) async throws -> CloudPage { CloudPage(changes: [], cursor: Data([1]), more: false) }
  func push(changes: [CloudChange]) async throws { lock.withLock { pushed.append(contentsOf: changes) } }
}

struct CloudBridgeTests {
  @Test
  func aLocalWriteReachesTheCloudAdapterThroughTheBridge() async throws {
    let database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: cloudModels, transport: nil)
    let cloud = RecordingCloud()
    let link = try database.connectCloud(io: cloud)
    _ = try await database.create(model: "Note", valuesJson: #"{"title":"hello"}"#, argsJson: "{}")
    try await link.syncNow()
    let titles = cloud.changes.filter { $0.field == "title" }
    #expect(titles.count == 1)
    #expect(titles.first?.value == .text(value: "hello"))
    #expect(link.state() == .idle)
    link.stop()
  }
}
