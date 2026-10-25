import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func declaredChangeEventsReachTheTypedStreamAndTheStreamEndsOnCancel() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]"#)
    let events = Container.events
    let cursor = Cursor(events)
    await eventually("the change subscription opens") { transport.calls.count == 1 }
    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"DELETED","recordId":"c1"}]}}}"#)
    guard case .event(let seen)? = await cursor.next("the removal event") else {
      Issue.record("the removal event did not arrive")
      return
    }
    #expect(seen.area == "CONTAINER" && seen.kind == "DELETED" && seen.id == "c1")
    #expect(seen.before == nil && seen.after == nil)
    #expect(transport.calls.map(\.operationName) == ["RecordEvents"])

    events.cancel()
    let ended = await cursor.next("the end of the cancelled stream")
    #expect(ended == nil)
  }
}
