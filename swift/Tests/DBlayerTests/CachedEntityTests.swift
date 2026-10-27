import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor
  func coldAndReleasedEntitiesOpenUntilTheirCoreSnapshotArrives() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "cached")
    var held: Entity<Moment>? = Moment.entity(row.id)
    #expect(held?.phase == .opening && held?.value == nil)
    try await held?.waitForInitialLoad()
    #expect(held?.value?.text == "cached")
    weak var old = held
    held = nil
    await eventually("the entity releases") { old == nil }
    let returned = Moment.entity(row.id)
    #expect(returned.phase == .opening)
    try await returned.waitForInitialLoad()
    #expect(returned.value?.text == "cached")
  }

  @Test @MainActor
  func repeatedGeneratedReadsKeepTheLiveObjectAndSnapshot() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "cached")
    let first = Moment.entity(row.id)
    try await first.waitForInitialLoad()
    let before = Entities<Moment>.shared.decodedRows
    for _ in 0..<5 { #expect(Moment.entity(row.id) === first) }
    #expect(Entities<Moment>.shared.decodedRows == before)
    #expect(first.value?.text == "cached")
  }

  @Test @MainActor
  func releasingTheOwnerOfEntitiesReturnsTheSubscriptionCensusToItsBase() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let rows = [try await Moment.create(text: "one"), try await Moment.create(text: "two")]
    let base = try DB.subscriptionCensus().subscriptions
    var owner: [Entity<Moment>]? = rows.map { Moment.entity($0.id) }
    await eventually("both entities read") { owner?.allSatisfy { $0.value != nil } == true }
    await eventually("one observer holds both ids") { (try? DB.subscriptionCensus().subscriptions) == base + 1 }
    owner = nil
    await eventually("the observer releases") { (try? DB.subscriptionCensus().subscriptions) == base }
    let again = Moment.entity(rows[0].id)
    try await again.waitForInitialLoad()
    #expect(again.value?.text == "one")
  }
}
