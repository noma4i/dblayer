import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct StorageTests {
  @Test
  func storageRulesAndWritesFollowTheSharedCases() async throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let cases = try JSONDecoder().decode([JSONValue].self, from: Data(contentsOf: root.appendingPathComponent("fixtures/storage.json")))
    for item in cases {
      let fields = item.objectValue
      let name = fields["name"]!.stringValue!
      let models = fields["models"]!.text
      if fields["error"] == .bool(true) {
        #expect(throws: (any Error).self, Comment(rawValue: name)) {
          try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: models, transport: nil)
        }
        continue
      }
      let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
      try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
      defer { try? FileManager.default.removeItem(at: directory) }
      let config = Config(path: directory.appendingPathComponent("db.sqlite").path, dataVersion: nil, cacheRows: 32)
      let transport = MockTransport()
      DB.database = try Database.open(config: config, modelsJson: models, transport: transport)
      let state: SyncState = name == "local" ? .synced : .pending
      let changes = Cursor(await Query<StorageRow>().live.stream())
      #expect(await changes.next()?.isEmpty == true)
      let row = try await StorageRow.create(record: "one")
      #expect(row.sync.state == state, Comment(rawValue: name))
      #expect(await changes.next()?.first?.record == "one")
      let edited = try await row.update(record: "two")
      #expect(edited.sync.state == state, Comment(rawValue: name))
      #expect(await changes.next()?.first?.record == "two")
      try await DB.database.flush()
      #expect(transport.calls.isEmpty, Comment(rawValue: name))
      DB.database = try Database.open(config: config, modelsJson: models, transport: transport)
      let reopened = try #require(await StorageRow.find(row.id).current())
      #expect(reopened.record == "two" && reopened.sync.state == state, Comment(rawValue: name))
      let deletes = Cursor(await Query<StorageRow>().live.stream())
      #expect(await deletes.next()?.count == 1)
      try await reopened.destroy()
      #expect(await deletes.next()?.isEmpty == true)
      try await DB.database.flush()
      #expect(transport.calls.isEmpty, Comment(rawValue: name))
    }
  }
}
