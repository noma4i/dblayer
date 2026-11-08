import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func aQueryReadsLimitRowsAfterOffsetRowsInOrder() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    for text in ["a", "b", "c", "d"] { _ = try await Moment.create(text: text, in: DB.account()).optimisticValue! }
    let window = Query<Moment>().order(Order(field: "text", direction: .asc)).limit(2).offset(1)
    #expect(try await window.live.current().map(\.text) == ["b", "c"])
  }
}
