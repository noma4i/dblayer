import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor func aQueryReadsLimitRowsAfterOffsetRowsInOrder() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON)
    for text in ["a", "b", "c", "d"] { _ = try await Moment.send(in: DB.account(), text: text).optimisticValue! }
    let window = Query<Moment>().order(Order(field: "text", direction: "asc")).limit(2).offset(1)
    #expect(try await window.live.current().map(\.text) == ["b", "c"])
  }
}
