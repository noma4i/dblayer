import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func chatEventPayloadPlatforms() async throws {
    for eventsFirst in [false, true] { try await checkEventPayload(eventsFirst) }
  }

  private func checkEventPayload(_ eventsFirst: Bool) async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"},{"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision lists } } }"},{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } message { id text chatId } } } }"}]"#)
    let scope = try DB.account()
    var streams: (Cursor<EventStream<Chat>>, Cursor<EventStream<Message>>, ValueStream, ValueStream)?
    if eventsFirst {
      let chats = scope.events(Chat.self)
      let messages = scope.events(Message.self)
      streams = (await opened(chats), await opened(messages), chats.stream, messages.stream)
    }
    transport.enqueue("ChatList", reply: #"{"data":{"chats":[]}}"#)
    let list = try Chat.list("PRIMARY")
    try await list.load()
    var secondary: Query<Chat>?
    if eventsFirst {
      transport.enqueue("ChatList", reply: #"{"data":{"chats":[{"__typename":"Chat","id":"c1","title":"before","revision":9007199254740993,"lists":["SECONDARY"]}]}}"#)
      secondary = try Chat.list("SECONDARY")
      try await secondary?.load()
    }
    transport.enqueue("ChatNode", reply: #"{"data":{"node":{"__typename":"Chat","id":"c1","title":"before","revision":9007199254740993,"lists":["SECONDARY"]}}}"#)
    let before = try await Chat.node("c1", force: true)
    #expect(before?.serverRevision == 9007199254740993)
    if let secondary { #expect(try await secondary.read().map(\.id) == ["c1"]) }
    if !eventsFirst {
      let chats = scope.events(Chat.self)
      let messages = scope.events(Message.self)
      streams = (await opened(chats), await opened(messages), chats.stream, messages.stream)
    }
    let (chatEvents, messageEvents, chatStream, messageStream) = try #require(streams)
    _ = (chatStream, messageStream)
    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":"9007199254740994","lists":["PRIMARY"]},"message":{"id":"m1","text":"received","chatId":"c1"}}]}}}"#)
    guard case .event(let changed)? = await chatEvents.next("chat payload event"),
          case .event(let received)? = await messageEvents.next("message payload event") else {
      Issue.record("payload events did not arrive")
      return
    }
    #expect(changed.id == "c1")
    #expect(changed.before?.title == "before")
    #expect(changed.after?.title == "after")
    #expect(changed.after?.serverRevision == 9007199254740994)
    #expect(received.after?.text == "received")
    #expect(received.after?.chatId == "c1")
    let rows = try await list.live.current()
    #expect(rows.map(\.id) == ["c1"])
    #expect(rows.first?.title == "after")
    if let secondary { #expect(try await secondary.read().isEmpty) }
    let expected = eventsFirst ? ["RecordEvents", "ChatList", "ChatList", "ChatNode"] : ["ChatList", "ChatNode", "RecordEvents"]
    #expect(transport.calls.map(\.operationName) == expected)
    await DB.closeForCleanup()
  }
}
