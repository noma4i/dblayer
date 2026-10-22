import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test
  func landsDeclarationAndRowCrossTheBridge() async throws {
    let schema = #"[{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}}],"actions":[{"name":"create","root_shape":"upsert","mutation":"CreateRecord","root":"createRecord.record","has_optimistic":true,"has_temp_row_retention":true,"lands":[{"model":"Record","root":"createRecord.extra"}]}]}]"#
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: #"{"data":{"createRecord":{"record":{"id":"r1"},"extra":{"id":"r2"}}}}"#)
    let database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: schema, transport: transport)
    try database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord { createRecord { record { id } extra { id } } }"}]"#)
    let row = try await database.create(model: "Record", valuesJson: "{}", argsJson: "{}")
    try await database.settled(model: "Record", rowId: row.id)
    try await database.warm(queryJson: #"{"model":"Record"}"#)
    #expect(try database.peekRow(model: "Record", rowId: "r2")?.id == "r2")
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
