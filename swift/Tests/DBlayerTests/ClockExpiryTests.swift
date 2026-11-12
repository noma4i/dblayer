import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func advancingTheTestClockExpiresAMemoryRowWithoutWaiting() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    let clock = try await openSignedClocked(modelsJson: schema(onlySubscription: "BulletinAdded"), transport: transport, operations: #"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]"#)
    let notices = Cursor(try await Notice.recent.live.stream())
    #expect(await notices.next()?.isEmpty == true)
    await eventually("the notice subscription opens") { transport.calls.contains { $0.operationName == "BulletinAdded" } }
    try transport.emit("BulletinAdded", json: #"{"data":{"bulletinAdded":{"body":"hello","occurredAt":"2026-10-18T00:00:00Z"}}}"#)
    #expect(await notices.first("the notice row") { !$0.isEmpty } != nil)

    clock.advance(by: 60)

    #expect(await notices.first("the notice expires") { $0.isEmpty } != nil)
  }
}
