import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// Model events through the generated API: what a collector of `scope.events(Model.self)` hears, and in which order.
extension BridgeProbeTests {
  private static func screenshot(_ chat: String) -> String {
    #"{"data":{"chatActionSignal":{"chatId":"\#(chat)","userId":"u","kind":"SCREENSHOT","active":true}}}"#
  }

  private static func openNoticeRig() async throws -> MockTransport {
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: schema(onlySubscription: "ChatActionSignal"), transport: transport, operations: #"[{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"}]"#)
    return transport
  }

  private static func position(_ item: ModelEventItem<ChatActivityNotice>?) -> UInt64? {
    guard case .event(let event)? = item else { return nil }
    return event.position
  }

  /// Requirement: a collector hears every event after it started, in the order the server sent them, and none from before. Observable: the chat ids the collector received.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aModelEventsCollectorHearsEventsInOrderAndNothingFromBeforeItStarted() async throws {
    let transport = try await Self.openNoticeRig()
    let early = await opened(try DB.account().events(ChatActivityNotice.self))
    await eventually("the signal subscription opens") { transport.calls.count == 1 }
    try transport.emit("ChatActionSignal", json: Self.screenshot("c1"))
    #expect(Self.position(await early.next("the first event")) == 1)

    let late = await opened(try DB.account().events(ChatActivityNotice.self))
    try transport.emit("ChatActionSignal", json: Self.screenshot("c2"))
    try transport.emit("ChatActionSignal", json: Self.screenshot("c3"))

    #expect(Self.position(await late.next("the first event of the late collector")) == 1, "the late collector got the past event or lost the next one")
    #expect(Self.position(await late.next("the second event of the late collector")) == 2)
    #expect(Self.position(await early.next("the second event of the early collector")) == 2)
    #expect(Self.position(await early.next("the third event of the early collector")) == 3)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func dlv_reader_cancel_ends_swift() async throws {
    _ = try await Self.openNoticeRig()
    let waiting = Done()
    let reader = Task { () throws -> Int in
      var events = 0
      for try await item in try DB.account().events(ChatActivityNotice.self) {
        switch item {
        case .ready: waiting.set()
        case .event: events += 1
        default: break
        }
      }
      return events
    }
    await eventually("the reader waits for the next event") { waiting.value }
    reader.cancel()
    #expect(try await valueBeforeDeadline(reader, release: {}) == 0, "the cancelled reader heard an event")
  }
}
