import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor
  func droppingAnEntitySourceReleasesItsObserver() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "held")
    let delivered = Counter()
    var source: CoreEntitySource<Moment>? = CoreEntitySource()
    source?.deliver = { _ in delivered.add() }
    weak let released = source
    source?.retain([row.id])
    await eventually("the entity source delivers its row") { delivered.value > 0 }
    source = nil
    await eventually("the entity source releases its observer") { released == nil && (try? DB.subscriptionCensus().subscriptions) == 0 }
  }
}
