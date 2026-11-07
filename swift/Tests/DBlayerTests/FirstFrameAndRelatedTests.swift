import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// Spec B, first frame: the cached rows come with the first value, read off the calling thread. Related queries: a stream over a server relation follows a change of its rows and of the link.
extension BridgeProbeTests {
  /// Requirement: a stream opened over cached rows delivers them in its very first value, which the reader takes off the main thread. Observable: the kind, the rows and the revision of the first value, and the thread that took it.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theFirstFrameOfAWarmStreamArrivesOffTheCallingThreadWithTheCachedRows() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: MockTransport())
    let ann = try await User.create(name: "Ann").optimisticValue!
    let seats = [try await Presence.create(userId: ann.id, room: "r").optimisticValue!, try await Presence.create(userId: ann.id, room: "r").optimisticValue!]
    let stream = try DB.account().stream(Presence.where(.equal("room", .string("r"))))

    let first = await Task.detached { () -> (onMain: Bool, ids: [String]?, revision: Int64?) in
      guard case let .snapshot(revision, rows, _, _)? = await Cursor(stream).next("the first value of the stream") else { return ((pthread_main_np() == 1), nil, nil) }
      return ((pthread_main_np() == 1), rows.map(\.id), revision)
    }.value
    await stream.cancel()

    #expect(!first.onMain, "the first value was taken on the calling thread")
    #expect(first.revision != nil, "the first value was not a snapshot with a revision")
    #expect(Set(first.ids ?? []) == Set(seats.map(\.id)), "the first value lacked the cached rows: \(String(describing: first.ids))")
  }

  /// Requirement: a stream over a server relation shows the rows of the relation, follows a change of one of its rows and a change of the link that moves a row out of it. Observable: the titles the projection shows after each step.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aRelatedQueryReturnsTheLinkedRowsAndFollowsATargetChangeAndALinkChange() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"},{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } message { id text chatId } } } }"}]"#)
    let scope = try DB.account()
    let chats = await opened(scope.events(Chat.self))
    await eventually("the events subscription opens") { transport.calls.contains { $0.operationName == "RecordEvents" } }
    transport.enqueue("ChatList", reply: #"{"data":{"chats":[{"__typename":"Chat","id":"c1","title":"before","revision":1,"lists":["SECONDARY"]}]}}"#)
    let related = Projected<Chat>(try scope.stream(try Chat.list("SECONDARY")))
    try await Chat.list("SECONDARY").load()
    await eventually("the linked row shows") { related.rows.map(\.title) == ["before"] }

    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":2,"lists":["SECONDARY"]},"message":{"id":"m1","text":"x","chatId":"c1"}}]}}}"#)
    await eventually("a change of the row shows") { related.rows.map(\.title) == ["after"] }

    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":3,"lists":["PRIMARY"]},"message":{"id":"m1","text":"x","chatId":"c1"}}]}}}"#)
    await eventually("a change of the link moves the row out of the relation") { related.rows.isEmpty }
    withExtendedLifetime(chats) {}
  }
}
