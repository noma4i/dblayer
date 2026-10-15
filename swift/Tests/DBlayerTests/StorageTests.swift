import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// Uses its own `Database`, never `DB.database`, so suites that share the global one keep it.
@Suite(.serialized)
struct StorageTests {
  static func rows(_ database: Database) async throws -> [WireRow] {
    let live = try database.liveQuery(queryJson: #"{"model":"StorageRow"}"#, listener: ChangeListener { _ in })
    return try await live.current().rows
  }

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
      var database = try Database.open(config: config, modelsJson: models, transport: transport)
      let state: WireSyncState = name == "local" ? .synced : .pending
      func syncState(_ row: WireRow) -> WireSyncState { row.sync.state }
      #expect(try await Self.rows(database).isEmpty, Comment(rawValue: name))
      let created = try await database.create(model: "StorageRow", valuesJson: #"{"record":"one"}"#, argsJson: "{}")
      let id = created.id
      #expect(syncState(created) == state, Comment(rawValue: name))
      #expect(try await Self.rows(database).first?.cells.first == .text(value: "one"), Comment(rawValue: name))
      let edited = try await database.update(model: "StorageRow", rowId: id, patchJson: #"{"record":"two"}"#, argsJson: "{}")
      #expect(syncState(edited) == state, Comment(rawValue: name))
      #expect(try await Self.rows(database).first?.cells.first == .text(value: "two"), Comment(rawValue: name))
      try await database.flush()
      #expect(transport.calls.isEmpty, Comment(rawValue: name))
      database = try Database.open(config: config, modelsJson: models, transport: transport)
      let reopened = try #require(await Self.rows(database).first, Comment(rawValue: name))
      #expect(reopened.cells.first == .text(value: "two") && syncState(reopened) == state, Comment(rawValue: name))
      try await database.destroy(model: "StorageRow", rowId: id)
      #expect(try await Self.rows(database).isEmpty, Comment(rawValue: name))
      try await database.flush()
      #expect(transport.calls.isEmpty, Comment(rawValue: name))
    }
  }
}
