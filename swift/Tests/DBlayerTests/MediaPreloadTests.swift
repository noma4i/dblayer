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
  func aMediaRefetchAfterMutationReadsTheWholeStaleWindow() async throws {
    try await DB.close()
    var models = try #require(JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as? [[String: Any]])
    let momentIndex = try #require(models.firstIndex { $0["key"] as? String == "Moment" })
    let mediaIndex = try #require(models.firstIndex { $0["key"] as? String == "MediaMessage" })
    let actions = try #require(models[momentIndex]["actions"] as? [[String: Any]])
    var action = try #require(actions.first { $0["name"] as? String == "like" })
    action["refetch"] = ["visual_media"]
    action["optimistic"] = [:] as [String: Any]
    models[mediaIndex]["actions"] = [action]
    var relations = try #require(models[mediaIndex]["relations"] as? [[String: Any]])
    let relationIndex = try #require(relations.firstIndex { $0["name"] as? String == "visual_media" })
    let binding: [String: Any] = ["chatId": ["from": "param", "name": "chatId"]]
    relations[relationIndex]["media"] = ["marker": "MediaMarker", "from": "node.mediaLibrary", "fields": ["revision", "visualCount", "audioCount", "giftCount"], "sequence": "latestSequence", "tail": "MediaTail", "after": "afterSequence", "marker_variables": binding, "tail_variables": binding]
    models[mediaIndex]["relations"] = relations
    let modelJSON = try #require(String(data: JSONSerialization.data(withJSONObject: models), encoding: .utf8))
    let transport = MockTransport()
    func marker(_ count: Int) -> String { #"{"data":{"node":{"mediaLibrary":{"revision":1,"visualCount":\#(count),"audioCount":0,"giftCount":0,"latestSequence":1}}}}"# }
    func page(_ id: String) -> String { #"{"data":{"node":{"messages":{"edges":[{"cursor":"\#(id)","node":{"__typename":"MediaMessage","id":"\#(id)","chatId":"c1","kind":"TEXT","mediaKind":"IMAGE","sequenceNumber":1}}],"pageInfo":{"hasNextPage":false,"endCursor":"\#(id)"}}}}}"# }
    for count in [0, 0, 1, 1, 1] { transport.enqueue("MediaMarker", reply: marker(count)) }
    transport.enqueue("ChatMedia", reply: page("v1"))
    transport.enqueue("ChatMedia", reply: page("v2"))
    transport.enqueue("LikeMoment", reply: #"{"data":{"likeMoment":{"__typename":"MediaMessage","id":"v1","chatId":"c1","kind":"TEXT","mediaKind":"IMAGE","sequenceNumber":1}}}"#)
    let definitions = #"[{"name":"MediaMarker","document":"query MediaMarker($chatId: ID!, $upToSequence: Int!) { node(id: $chatId) { mediaLibrary(upToSequence: $upToSequence) { revision visualCount audioCount giftCount latestSequence } } }"},{"name":"MediaTail","document":"query MediaTail($chatId: ID!, $afterSequence: Int!, $first: Int, $after: String) { node(id: $chatId) { messages(afterSequence: $afterSequence, first: $first, after: $after) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } }"},{"name":"LikeMoment","document":"mutation LikeMoment($momentId: ID!) { likeMoment(momentId: $momentId) { id chatId kind mediaKind sequenceNumber } }"}]"#
    try await DB.openTesting(transport: transport, modelsJSON: modelJSON, operations: [ChatMediaOperation.self], listening: false)
    try DB.database.registerOperations(defsJson: definitions)
    let query = try MediaMessage.visualMedia("c1")
    let (signals, signal) = AsyncStream<Int>.makeStream()
    let cursor = Cursor(signals)
    defer { signal.finish() }
    let first = Task { do { try await query.loadAll(); signal.yield(1) } catch { signal.yield(9) } }
    defer { first.cancel() }
    guard await cursor.next("the initial media load") == 1 else { Issue.record("the initial media load failed"); return }
    let row = try #require(try await query.read().first)
    let mutation = Task {
      do {
        let receipt = try await row.act("like", values: [:], args: [:])
        if case .confirmed = try await receipt.outcome() { signal.yield(2) } else { signal.yield(9) }
      } catch { signal.yield(9) }
    }
    defer { mutation.cancel() }
    guard await cursor.next("the mutation outcome") == 2 else { Issue.record("the mutation was not confirmed"); return }
    let reread = Task { do { try await query.loadAll(); signal.yield(3) } catch { signal.yield(9) } }
    defer { reread.cancel() }
    guard await cursor.next("the stale media load") == 3 else { Issue.record("the stale media load failed"); return }
    #expect(try await query.read().map(\.id) == ["v2"])
    #expect(transport.calls.filter { $0.operationName == "ChatMedia" }.count == 2)
    let repeatRead = Task { do { try await query.loadAll(); signal.yield(4) } catch { signal.yield(9) } }
    defer { repeatRead.cancel() }
    guard await cursor.next("the fresh media load") == 4 else { Issue.record("the fresh media load failed"); return }
    #expect(transport.calls.filter { $0.operationName == "MediaMarker" }.count == 5)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func preloadsAndThreeMediaLoadsFinishOnEmptyPages() async throws {
    try await DB.close()
    let transport = MockTransport()
    let preload = transport.hold("PresenceOnline", reply: #"{"data":{"presenceOnline":[]}}"#)
    defer { preload.release() }
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, operations: [MediaPresenceOnline.self, ChatMediaOperation.self], preload: true)
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
      held.stream.cancel()
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
    defer { held.stream.cancel() }
    try await held.loaded()
    let down = transport.hold("ChatMedia", failure: .Network(detail: "down"))
    async let failing: Void = failedQuery.loadAll()
    try await networkDrops(during: down)
    do { try await failing; Issue.record("the failed page returned successfully") }
    catch DbError.Network {} catch { Issue.record("unexpected error: \(error)") }
    guard await observe("the failed media page arrives", { held.error != nil }) else { return }
    #expect(held.rows.map(\.id) == ["x1"])
    #expect(held.phase == .loaded)
    #expect(held.state?.complete == false)
    if case .Network? = held.error {} else { Issue.record("the network error is missing") }
  }
}
