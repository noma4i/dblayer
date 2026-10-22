import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test
  func memoryNoticesReachTheGeneratedLiveQueryWithoutAReadRequest() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]"#)
    let notices = Cursor(await Notice.recent.live.stream())
    #expect(await notices.next()?.isEmpty == true)
    await eventually("the notice subscription opens") { transport.calls.count == 1 }
    try transport.emit("BulletinAdded", json: #"{"data":{"bulletinAdded":{"body":"hello","occurredAt":"2026-10-18T00:00:00Z"}}}"#)
    let delivered = await notices.first("the notice row") { !$0.isEmpty }
    #expect(delivered?.first?.body == "hello")
    #expect(delivered?.first?.id.isEmpty == false)
    #expect(transport.calls.map(\.operationName) == ["BulletinAdded"])
  }
}
