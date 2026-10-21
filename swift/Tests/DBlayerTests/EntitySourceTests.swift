import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor
  func aReplacedDatabaseCannotDeliverRowsOrRemovalsToCurrentEntities() async throws {
    let old = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    DB.database = old
    let former = try await Moment.create(values: ["id": .string("same"), "text": .string("old")])
    let registry = Entities<Moment>()
    let previous = registry.entity(former.id)
    let current = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    DB.database = current
    let row = try await Moment.create(values: ["id": .string("same"), "text": .string("current")])
    try await Moment.recent.warm()
    let entity = registry.entity(row.id)
    #expect(previous !== entity && previous.value == nil)
    for removal in [false, true] {
      registry.receive(from: old, delivery: EntityDecoding.Decoded(revision: .max, rows: removal ? [] : [(former, UInt64(1))], removed: removal ? [former.id] : []))
      #expect(entity.value?.text == "current")
    }
    registry.receive(from: current, delivery: EntityDecoding.Decoded(revision: .max, rows: [], removed: [row.id]))
    #expect(entity.value == nil)
  }

  @Test @MainActor
  func droppingAnEntitySourceReleasesItsObserver() async throws {
    DB.database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "held")
    let delivered = Counter()
    var source: CoreEntitySource<Moment>? = CoreEntitySource()
    source?.deliver = { _, _ in delivered.add() }
    weak var released = source
    source?.retain([row.id])
    await eventually("the entity source delivers its row") { delivered.value > 0 }
    source = nil
    await eventually("the entity source releases its observer") { released == nil && DB.subscriptionCensus().subscriptions == 0 }
  }
}
