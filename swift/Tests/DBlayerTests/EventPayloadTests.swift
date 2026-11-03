import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func chatEventPayloadPlatforms() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"},{"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision lists } } }"},{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } message { id text chatId } } } }"}]"#)
    transport.enqueue("ChatList", reply: #"{"data":{"chats":[]}}"#)
    let list = try Chat.list("PRIMARY")
    try await list.load()
    transport.enqueue("ChatNode", reply: #"{"data":{"node":{"__typename":"Chat","id":"c1","title":"before","revision":9007199254740993,"lists":["SECONDARY"]}}}"#)
    let before = try await Chat.node("c1", force: true)
    #expect(before?.serverRevision == 9007199254740993)
    let scope = try DB.account()
    let chats = scope.events(Chat.self)
    let messages = scope.events(Message.self)
    defer { chats.stream.cancel(); messages.stream.cancel() }
    let chatEvents = Cursor(chats)
    let messageEvents = Cursor(messages)
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
    #expect(transport.calls.map(\.operationName) == ["ChatList", "ChatNode", "RecordEvents"])
  }
}
