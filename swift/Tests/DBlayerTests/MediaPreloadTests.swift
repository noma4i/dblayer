import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private enum MediaPresenceOnline: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "PresenceOnline"
  static let document = "query PresenceOnline { presenceOnline { id userId room } }"
}

private enum ChatMediaOperation: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "ChatMedia"
  static let document = "query ChatMedia($chatId: ID!, $first: Int, $after: Cursor, $mediaKinds: [String!], $includeGifts: Boolean!) { node(id: $chatId) { ... on Chat { messages(first: $first, after: $after, mediaKinds: $mediaKinds, includeGifts: $includeGifts) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } } }"
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func preloadsAndThreeMediaLoadsFinishOnEmptyPages() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    let preload = transport.hold("PresenceOnline", reply: #"{"data":{"presenceOnline":[]}}"#)
    defer { preload.release() }
    let operations: [any GraphQLOperation.Type] = [MediaPresenceOnline.self, ChatMediaOperation.self]
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: JSONValue.array(operations.map { .object(["name": .string($0.operationName), "document": .string($0.document)]) }).text)
    let (signals, signal) = AsyncStream<Void>.makeStream()
    let entered = Task { await preload.entered(); signal.yield(()) }
    defer { entered.cancel(); signal.finish() }
    guard await Cursor(signals).next("the preload request", timeout: .seconds(20)) != nil else { return }
    preload.release()
    let empty = #"{"data":{"node":{"messages":{"edges":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"#
    let cases: [(Query<MediaMessage>, String, String, String)] = [
      (try MediaMessage.visualMedia("c1"), "v1", "TEXT", #""IMAGE""#),
      (try MediaMessage.audioMedia("c1"), "a1", "TEXT", #""AUDIO""#),
      (try MediaMessage.giftMedia("c1"), "g1", "GIFT", "null"),
    ]
    for (query, id, kind, mediaKind) in cases {
      let first = #"{"data":{"node":{"messages":{"edges":[{"cursor":"\#(id)","node":{"__typename":"MediaMessage","id":"\#(id)","chatId":"c1","kind":"\#(kind)","mediaKind":\#(mediaKind),"sequenceNumber":1}}],"pageInfo":{"hasNextPage":true,"endCursor":"\#(id)"}}}}}"#
      transport.enqueue("ChatMedia", reply: first)
      transport.enqueue("ChatMedia", reply: empty)
      try await query.loadAll()
      #expect(try await query.read().map(\.id) == [id])
      let held = Projected<MediaMessage>(try DB.account().stream(query))
      try await held.loaded()
      #expect(held.state?.complete == true)
      await held.stream.cancel()
    }
    let calls = transport.calls.filter { $0.operationName == "ChatMedia" }
    #expect(calls.count == 6)
    guard calls.count == 6 else { return }
    for (index, id) in ["v1", "a1", "g1"].enumerated() {
      let first = try #require(JSONSerialization.jsonObject(with: Data(calls[index * 2].variablesJson.utf8)) as? [String: Any])
      let second = try #require(JSONSerialization.jsonObject(with: Data(calls[index * 2 + 1].variablesJson.utf8)) as? [String: Any])
      #expect(first["after"] == nil)
      #expect(second["after"] as? String == id)
    }
    #expect(transport.calls.filter { $0.operationName == "PresenceOnline" }.count == 1)
    transport.enqueue("ChatMedia", reply: #"{"data":{"node":{"messages":{"edges":[{"cursor":"x1","node":{"__typename":"MediaMessage","id":"x1","chatId":"c2","kind":"GIFT","mediaKind":null,"sequenceNumber":1}}],"pageInfo":{"hasNextPage":true,"endCursor":"x1"}}}}}"#)
    let failedQuery = try MediaMessage.giftMedia("c2")
    let held = Projected<MediaMessage>(try DB.account().stream(failedQuery))
    try await held.loaded()
    let down = transport.hold("ChatMedia", failure: .Network(detail: "down"))
    transport.enqueue("ChatMedia", reply: empty)
    let failing = Task { try await failedQuery.loadAll() }
    let finished = Done()
    let watcher = Task { _ = await failing.result; finished.set() }
    defer { watcher.cancel() }
    try await networkDrops(during: down)
    down.release()
    await eventually("the held page reached the transport") { transport.calls.filter { $0.operationName == "ChatMedia" }.count == 8 }
    #expect(!finished.value, "a media load without network keeps waiting")
    try DB.database.setOnline(online: true)
    try await valueBeforeDeadline(failing) {}
    #expect(transport.calls.filter { $0.operationName == "ChatMedia" }.count == 9)
    guard await observe("the media window is complete", { held.state?.complete == true }) else { return }
    #expect(held.rows.map(\.id) == ["x1"])
    #expect(held.phase == .loaded)
    #expect(held.error == nil)
  }
}
