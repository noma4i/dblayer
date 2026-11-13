import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func oneSignalRoutesTypedActionsAndNotices() async throws {
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil), modelsJson: schema(onlySubscription: "ChatActionSignal"), transport: transport, operations: #"[{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"}]"#)
    let events = Cursor(try DB.account().events(ChatActivityNotice.self))
    let actions = Cursor(try await ChatAction.byChat("c").live.stream())
    #expect(await actions.next()?.isEmpty == true)
    await eventually("the signal subscription opens") { transport.calls.count == 1 }
    try transport.emit("ChatActionSignal", json: #"{"data":{"chatActionSignal":{"chatId":"c","userId":"u","kind":"TYPING","active":true}}}"#)
    #expect(await actions.first("typing") { !$0.isEmpty }?.first?.kind == "TYPING")
    try transport.emit("ChatActionSignal", json: #"{"data":{"chatActionSignal":{"chatId":"c","userId":"u","kind":"SCREENSHOT","active":true}}}"#)
    guard case .event(let notice)? = await events.first("screenshot", where: { if case .event = $0 { true } else { false } }) else {
      Issue.record("screenshot did not arrive")
      return
    }
    #expect(notice.after?.kind == "SCREENSHOT")
    try transport.emit("ChatActionSignal", json: #"{"data":{"chatActionSignal":{"chatId":"c","userId":"u","kind":"TYPING","active":false}}}"#)
    #expect(await actions.first("typing stops") { $0.isEmpty }?.isEmpty == true)
    #expect(transport.calls.filter { $0.operationName == "ChatActionSignal" }.count == 1)
  }

  @Test
  func memoryNoticesReachTheGeneratedLiveQueryWithoutAReadRequest() async throws {
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil), modelsJson: schema(onlySubscription: "BulletinAdded"), transport: transport, operations: #"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]"#)
    let notices = Cursor(try await Notice.recent.live.stream())
    #expect(await notices.next()?.isEmpty == true)
    await eventually("the notice subscription opens") { transport.calls.count == 1 }
    try transport.emit("BulletinAdded", json: #"{"data":{"bulletinAdded":{"body":"hello","occurredAt":"2026-10-18T00:00:00Z"}}}"#)
    let delivered = await notices.first("the notice row") { !$0.isEmpty }
    #expect(delivered?.first?.body == "hello")
    #expect(delivered?.first?.id.isEmpty == false)
    #expect(transport.calls.map(\.operationName) == ["BulletinAdded"])
  }
}
