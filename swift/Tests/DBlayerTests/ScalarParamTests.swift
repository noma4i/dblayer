import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let tailDocument = "query tailSince($chatId: ID!, $afterSequence: Int, $pinned: Boolean, $first: Int, $after: String) { tailSince(chatId: $chatId, afterSequence: $afterSequence, pinned: $pinned, first: $first, after: $after) { edges { cursor node { id text } } pageInfo { hasNextPage endCursor } } }"

  @Test
  func aScopeParamOfIntAndBoolIsSentAsItsJsonValue() {
    let query = Tail.since("c1", afterSequence: 120, pinned: true)
    #expect(query.relation == "since")
    #expect(query.params == ["chatId": .string("c1"), "afterSequence": .number(120), "pinned": .bool(true)])
    #expect(Tail.since("c1", afterSequence: 121, pinned: true).params != query.params)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aTypedScopeParamReachesTheServerAsANumberAndAKeyOfItsOwnWindow() async throws {
    let transport = MockTransport()
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("tailSince"), "document": .string(Self.tailDocument)])]).text)
    transport.enqueue("tailSince", reply: #"{"data":{"tailSince":{"edges":[{"cursor":"c1","node":{"id":"t1","text":"a"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c1"}}}}"#)

    try await Tail.since("c1", afterSequence: 120, pinned: false).refresh()

    let sent = transport.calls.first { $0.operationName == "tailSince" }?.variablesJson ?? ""
    #expect(JSONValue.parse(sent) == JSONValue.parse(#"{"chatId":"c1","afterSequence":120,"pinned":false,"first":50}"#), "variables sent: \(sent)")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anAggregateOperationReturnsItsFieldsAsAValue() async throws {
    let transport = try Self.openOperationRig("chatMedia", document: "query chatMedia($chatId: ID!, $upToSequence: Int) { chatMedia(chatId: $chatId, upToSequence: $upToSequence) { visualCount audioCount giftCount latestSequence } }", reply: #"{"data":{"chatMedia":{"visualCount":3,"audioCount":1,"giftCount":0,"latestSequence":120}}}"#)

    let result = try await Operations.chatMediaLoad(chatId: "c1", upToSequence: 120)

    #expect(result.visualCount == 3)
    #expect(result.latestSequence == 120)
    let sent = transport.calls.first { $0.operationName == "chatMedia" }?.variablesJson ?? ""
    #expect(JSONValue.parse(sent) == JSONValue.parse(#"{"chatId":"c1","upToSequence":120}"#), "variables sent: \(sent)")
  }
}
