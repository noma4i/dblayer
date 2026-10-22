import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor
  func cachedFirstReadAndRetainedNewerValueAreAvailableBeforeDelivery() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "cached")
    try await Moment.recent.warm()
    let cached = try #require(try DB.database.peekRow(model: "Moment", rowId: row.id))
    let registry = Entities<Moment>(source: CountingSource(cached: cached))
    var held: Entity<Moment>? = registry.entity(row.id)
    #expect(held?.value?.text == "cached")
    let newer = Moment(id: row.id, uuid: row.id, text: "newer", media: .object([:]), createdAt: "2026-10-08T00:00:00.000Z")
    registry.apply(EntityDecoding.Decoded(revision: cached.revision + 1, rows: [(newer, 1)], removed: []))
    #expect(registry.entity(row.id).value?.text == "newer")
    held = nil
    let returned = registry.entity(row.id)
    #expect(returned.value?.text == "newer")
    registry.apply(EntityDecoding.Decoded(revision: cached.revision, rows: [(row, 1)], removed: []))
    #expect(returned.value?.text == "newer")
    #expect(registry.entity("missing").value == nil)
  }
}
