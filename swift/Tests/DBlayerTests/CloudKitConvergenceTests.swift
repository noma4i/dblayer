import CloudKit
import Foundation
import Testing
@testable import DBlayer

private let cloudModels = #"[{"key":"Note","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"},"optional":true}],"storage":"local","cloud":["title"]}]"#
private let zone = CKRecordZone.ID(zoneName: CloudKitRecords.zoneName, ownerName: CKCurrentUserDefaultName)

private func titles(_ database: Database) async -> [String: String] {
  let live = database.liveQuery(queryJson: #"{"model":"Note"}"#, listener: ChangeListener { _ in })
  let rows = await live.current().rows
  return Dictionary(uniqueKeysWithValues: rows.compactMap { row in row.cells.compactMap { cell -> String? in if case .text(let value) = cell { return value } else { return nil } }.first.map { (row.id, $0) } })
}

struct CloudKitConvergenceTests {
  @Test
  func twoDatabasesConvergeThroughTheAdapterOnASharedStore() async throws {
    let store = FakeCloudKitStore()
    let first = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: cloudModels, transport: nil)
    let second = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: cloudModels, transport: nil)
    let firstLink = try first.connectCloud(io: CloudKitAdapter(wire: FakeCloudKitWire(store: store), zoneID: zone))
    let secondLink = try second.connectCloud(io: CloudKitAdapter(wire: FakeCloudKitWire(store: store), zoneID: zone))
    let created = try await first.create(model: "Note", valuesJson: #"{"title":"hello"}"#, argsJson: "{}")
    try await firstLink.syncNow()
    try await secondLink.syncNow()
    let id = created.id
    #expect(await titles(second) == [id: "hello"])
    _ = try await second.update(model: "Note", rowId: id, patchJson: #"{"title":"edited"}"#, argsJson: "{}")
    try await secondLink.syncNow()
    try await firstLink.syncNow()
    #expect(await titles(first) == [id: "edited"])
    firstLink.stop()
    secondLink.stop()
  }
}
