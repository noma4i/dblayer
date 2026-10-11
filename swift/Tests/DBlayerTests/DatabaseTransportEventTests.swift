import Foundation
import Testing
@testable import DBlayer

private final class RecordingSink: SubscriptionSinkProtocol, @unchecked Sendable {
  private let lock = NSLock()
  private var recorded: [String] = []

  var delivered: [String] { lock.withLock { recorded } }

  func closed() {}
  func ready() {}
  func failed(kind: String, detail: String) {}

  func event(json: String) throws {
    lock.withLock { recorded.append(json) }
  }
}

private final class EventTap: @unchecked Sendable {
  private let lock = NSLock()
  private var items: [String] = []

  func record(operationName: String, json: String) {
    lock.withLock { items.append("\(operationName)|\(json)") }
  }

  var all: [String] { lock.withLock { items } }
}

@Suite(.serialized)
struct DatabaseTransportEventTests {
  @Test
  func relayWrapsAServerFrameInTheResponseEnvelopeForTheCoreAndKeepsItBareForTheObserver() async throws {
    let frame = #"{"messageCreated":{"message":{"body":"doc probe","chatId":364161,"id":"478543","reactions":[],"userId":31222}}}"#
    let sink = RecordingSink()
    let tap = EventTap()
    let (events, continuation) = AsyncThrowingStream<GraphQLSubscriptionEvent, Error>.makeStream()
    continuation.yield(.ready)
    continuation.yield(.payload(JSONValue.parse(frame)))
    continuation.finish()

    await DatabaseTransport.relay(
      events: events,
      operationName: "messageCreated",
      sink: sink,
      onEvent: { name, json in tap.record(operationName: name, json: json) }
    )

    #expect(sink.delivered.map(JSONValue.parse) == [JSONValue.parse(#"{"data":"# + frame + "}")])
    #expect(tap.all.map { $0.replacingOccurrences(of: "messageCreated|", with: "") }.map(JSONValue.parse) == [JSONValue.parse(frame)])
  }
}
