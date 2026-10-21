import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test
  func landsAddsANewMessageToTheLoadedThread() async throws {
    var models = try #require(JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as? [[String: Any]])
    let index = try #require(models.firstIndex { $0["key"] as? String == "Message" })
    var actions = try #require(models[index]["actions"] as? [[String: Any]])
    actions.append(["name": "land", "root_shape": "upsert", "mutation": "LandMessage", "lands": [["model": "Message", "root": "landMessage.extra"]]])
    models[index]["actions"] = actions
    let transport = MockTransport()
    let page = [Self.stableNode("m1", 1, text: "t1"), Self.stableNode("m5", 5, text: "t5")]
    transport.enqueue("ChatThread", reply: #"{"data":{"chat":{"messages":{"nodes":[\#(page.joined(separator: ","))],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"#)
    let schema = String(decoding: try JSONSerialization.data(withJSONObject: models), as: UTF8.self)
    DB.database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: schema, transport: transport)
    try DB.database.registerOperations(defsJson: Self.stableOperations)
    try DB.database.registerOperations(defsJson: #"[{"name":"LandMessage","document":"mutation LandMessage { landMessage { extra { id chatId authorId text createdAt sequenceNumber } } }"}]"#)
    let thread = Message.thread("c1")
    try await thread.load()
    for _ in 0..<2 {
      transport.enqueue("LandMessage", reply: #"{"data":{"landMessage":{"extra":\#(Self.stableNode("m3", 3, text: "landed"))}}}"#)
      _ = try await DB.database.act(model: "Message", rowId: "m1", action: "land", patchJson: "{}", argsJson: "{}")
      try await DB.database.settled(model: "Message", rowId: "m1")
      try await DB.database.warm(queryJson: thread.json)
      #expect(try DB.database.peekQuery(queryJson: thread.json)?.rows.map(\.id) == ["m1", "m3", "m5"])
    }
  }

  fileprivate static func stableNode(_ id: String, _ sequence: Int, text: String) -> String {
    #"{"__typename":"Message","id":"\#(id)","chatId":"c1","authorId":"a","text":"\#(text)","createdAt":"2026-10-0\#(sequence)T00:00:00.000Z","sequenceNumber":\#(sequence)}"#
  }

  fileprivate static let stableOperations = #"[{"name":"ChatThread","document":"query ChatThread($chatId: ID!, $first: Int, $after: String) { chat(id: $chatId) { messages(first: $first, after: $after) { nodes { id chatId authorId text createdAt sequenceNumber } pageInfo { hasNextPage endCursor } } } }"},{"name":"CreateMessage","document":"mutation CreateMessage($chatId: ID!, $text: String, $signedId: String, $attachedUserId: ID) { createMessage(chatId: $chatId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id chatId authorId text createdAt sequenceNumber } }"}]"#

  @Test
  func aConfirmedMessageKeepsItsPlaceInTheThreadWindow() async throws {
    let transport = MockTransport()
    let page = [Self.stableNode("m1", 1, text: "t1"), Self.stableNode("m2", 2, text: "t2"), Self.stableNode("m5", 5, text: "t5")]
    transport.enqueue("ChatThread", reply: #"{"data":{"chat":{"messages":{"nodes":[\#(page.joined(separator: ","))],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"#)
    DB.database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: Self.stableOperations)
    let thread = Message.thread("c1")
    try await thread.load()
    let feed = ListFeed<Message>()
    let source = LiveSource<[Message]>(initial: []) { emit in
      let subscription = try DB.database.liveWindow(queryJson: thread.json, tail: 10, listener: ChangeListener { emit(feed.apply($0).rows) })
      return { subscription.cancel() }
    }
    let changes = Cursor(await source.stream())
    #expect(await changes.first("the loaded page") { $0.map(\.text) == ["t1", "t2", "t5"] } != nil)

    transport.enqueue("CreateMessage", reply: #"{"data":{"createMessage":{"__typename":"Message","id":"m9","chatId":"c1","authorId":"a","text":"mine","createdAt":"2026-10-09T00:00:00.000Z","sequenceNumber":3}}}"#)
    _ = try await Message.create(chatId: "c1", authorId: "a", text: "mine")
    let confirmed = await changes.first("the confirmed message after the rows shown before it") { $0.map(\.id).last == "m9" }
    #expect(confirmed?.map(\.text) == ["t1", "t2", "t5", "mine"])
  }
}
