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
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"}]"#)
    return transport
  }

  private static func chat(_ item: ModelEventItem<ChatActivityNotice>?) -> String? {
    guard case .event(let event)? = item else { return nil }
    return event.after?.chatId
  }

  /// Requirement: a collector hears every event after it started, in the order the server sent them, and none from before. Observable: the chat ids the collector received.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aModelEventsCollectorHearsEventsInOrderAndNothingFromBeforeItStarted() async throws {
    let transport = try await Self.openNoticeRig()
    let early = await opened(try DB.account().events(ChatActivityNotice.self))
    await eventually("the signal subscription opens") { transport.calls.count == 1 }
    try transport.emit("ChatActionSignal", json: Self.screenshot("c1"))
    #expect(Self.chat(await early.next("the first event")) == "c1")

    let late = await opened(try DB.account().events(ChatActivityNotice.self))
    try transport.emit("ChatActionSignal", json: Self.screenshot("c2"))
    try transport.emit("ChatActionSignal", json: Self.screenshot("c3"))

    #expect(Self.chat(await late.next("the first event of the late collector")) == "c2", "the late collector got the past event or lost the next one")
    #expect(Self.chat(await late.next("the second event of the late collector")) == "c3")
    #expect(Self.chat(await early.next("the second event of the early collector")) == "c2")
    #expect(Self.chat(await early.next("the third event of the early collector")) == "c3")
  }

  /// S209: a collector that reads nothing while 400 events arrive takes the events the journal kept and lost ranges that add up to the 400 sent. Observed: the items of the collector. Breaks when the journal drops an event without keeping its position.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aSlowCollectorReadsKeptEventsAndLostRangesThatAddUpToTheEventsSent() async throws {
    let transport = try await Self.openNoticeRig()
    let collector = await opened(try DB.account().events(ChatActivityNotice.self))
    await eventually("the signal subscription opens") { transport.calls.count == 1 }
    for index in 0..<400 { try transport.emit("ChatActionSignal", json: Self.screenshot("c\(index)")) }
    await eventually("the 400 notices are stored") { (try? await Query<ChatActivityNotice>().read().count) == 400 }
    var events: UInt64 = 0
    var lost: [(UInt64, UInt64)] = []
    while events + lost.reduce(0, { $0 + $1.1 - $1.0 + 1 }) < 400 {
      switch await collector.next("the next item of the burst") {
      case .event?: events += 1
      case let .lost(first, last)?: lost.append((first, last))
      default: Issue.record("the burst ended before 400 events were accounted"); return
      }
    }
    #expect(events + lost.reduce(0, { $0 + $1.1 - $1.0 + 1 }) == 400, "kept events and lost positions add up to the events sent")
    #expect(events <= 256, "the journal kept \(events) events of a burst of 400")
    #expect(!lost.isEmpty, "a reader that read nothing during the burst lost no position")
  }

  /// Requirement: a session reset ends the old events stream and nothing of the old session follows. A stream of the next session needs a real session with a model-event subscription, which the wrapper rigs cannot start: the core covers it at Sync level. Observable: the end of the old stream.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aSessionResetEndsTheOldEventsStream() async throws {
    let transport = try await Self.openNoticeRig()
    let first = await opened(try DB.account().events(ChatActivityNotice.self))
    await eventually("the signal subscription opens") { transport.calls.count == 1 }
    try transport.emit("ChatActionSignal", json: Self.screenshot("a1"))
    #expect(Self.chat(await first.next("the event of the first session")) == "a1")

    try await DB.sessionReset()
    let ended = await first.next("the end of the old stream", timeout: .seconds(10))
    #expect(ended == nil, "the old stream delivered after the session reset: \(String(describing: ended))")
  }
}
