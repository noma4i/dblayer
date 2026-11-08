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

  /// S209: a collector that reads nothing while 400 events arrive takes the events the journal kept and lost ranges that add up to the 400 sent. Observed: the items of the collector. Breaks when the journal drops an event without keeping its position.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aSlowCollectorReadsKeptEventsAndLostRangesThatAddUpToTheEventsSent() async throws {
    let transport = try await Self.openNoticeRig()
    let collector = await opened(try DB.account().events(ChatActivityNotice.self))
    await eventually("the signal subscription opens") { transport.calls.count == 1 }
    for burst in [0..<200, 200..<400] {
      for index in burst { try transport.emit("ChatActionSignal", json: Self.screenshot("c\(index)")) }
      try await rowsReach("the notices of the burst are stored", Query<ChatActivityNotice>()) { $0.count == burst.upperBound }
      #expect(try await Query<ChatActivityNotice>().read().count == burst.upperBound)
    }
    var events: UInt64 = 0
    var lost: [(UInt64, UInt64)] = []
    var stopped = false
    await consuming(collector, "the next item of the burst", until: { stopped || events + lost.reduce(0, { $0 + $1.1 - $1.0 + 1 }) >= 400 }) { item in
      switch item {
      case .event: events += 1
      case let .lost(first, last): lost.append((first, last))
      default: stopped = true
      }
    }
    #expect(events + lost.reduce(0, { $0 + $1.1 - $1.0 + 1 }) == 400, "kept events and lost positions add up to the events sent")
    #expect(events <= 256, "the journal kept \(events) events of a burst of 400")
    #expect(!lost.isEmpty, "a reader that read nothing during the burst lost no position")
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

  /// Requirement: a session reset ends the old events stream and nothing of the old session follows. A stream of the next session needs a real session with a model-event subscription, which the wrapper rigs cannot start: the core covers it at Sync level. Observable: the end of the old stream.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aSessionResetEndsTheOldEventsStream() async throws {
    let transport = try await Self.openNoticeRig()
    let first = await opened(try DB.account().events(ChatActivityNotice.self))
    await eventually("the signal subscription opens") { transport.calls.count == 1 }
    try transport.emit("ChatActionSignal", json: Self.screenshot("a1"))
    #expect(Self.position(await first.next("the event of the first session")) == 1)

    try await DB.sessionReset()
    let ended = await first.next("the end of the old stream", timeout: .seconds(10))
    guard case .ended(let cause)? = ended else {
      Issue.record("the old stream did not end with its cause after the session reset: \(String(describing: ended))")
      return
    }
    #expect(cause == .sessionEnded)
  }
}
