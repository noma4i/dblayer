import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor
  func streamDeliversCurrentThenNewestAndRemoval() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "A")
    let entity = Moment.entity(row.id)
    try await entity.waitForInitialLoad()
    let cursor = Cursor(entity.changes())
    #expect(await cursor.next("the current value")??.text == "A")
    _ = try await row.update(text: "B")
    _ = try await row.update(text: "C")
    await eventually("the newest value") { entity.value?.text == "C" }
    #expect(await cursor.next("the newest value")??.text == "C")
    try await row.destroy()
    await eventually("the removal") { entity.phase == .absent }
    let removed = await cursor.next("the absent value")
    #expect(removed != nil && removed! == nil)
  }

  @Test @MainActor
  func readersHoldOneEntityAndClosingEndsEveryStream() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "held")
    var entity: Entity<Moment>? = Moment.entity(row.id)
    try await entity?.waitForInitialLoad()
    weak var weakEntity = entity
    var first: LiveStream<Moment?>? = entity?.changes()
    var second: LiveStream<Moment?>? = entity?.changes()
    entity = nil
    #expect(weakEntity != nil)
    first?.cancel()
    first = nil
    #expect(weakEntity != nil)
    let cursor = Cursor(second!)
    #expect(await cursor.next("the held value")??.text == "held")
    try await DB.close()
    #expect(await cursor.next("the closed stream") == nil)
    second = nil
    await eventually("the closed streams release the entity") { weakEntity == nil }
  }
}
