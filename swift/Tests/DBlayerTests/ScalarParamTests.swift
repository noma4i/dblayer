import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let tailDocument = "query tailSince($containerId: ID!, $afterSequence: Int, $pinned: Boolean, $first: Int, $after: String) { tailSince(containerId: $containerId, afterSequence: $afterSequence, pinned: $pinned, first: $first, after: $after) { edges { cursor node { id text } } pageInfo { hasNextPage endCursor } } }"

  @Test
  func aScopeParamOfIntAndBoolIsSentAsItsJsonValue() {
    let query = Tail.since("c1", afterSequence: 120, pinned: true)
    #expect(query.relation == "since")
    #expect(query.params == ["containerId": .string("c1"), "afterSequence": .number(120), "pinned": .bool(true)])
    #expect(Tail.since("c1", afterSequence: 121, pinned: true).params != query.params)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aTypedScopeParamReachesTheServerAsANumberAndAKeyOfItsOwnWindow() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("tailSince"), "document": .string(Self.tailDocument)])]).text)
    transport.enqueue("tailSince", reply: #"{"data":{"tailSince":{"edges":[{"cursor":"c1","node":{"id":"t1","text":"a"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c1"}}}}"#)

    try await Tail.since("c1", afterSequence: 120, pinned: false).refresh()

    let sent = transport.calls.first { $0.operationName == "tailSince" }?.variablesJson ?? ""
    #expect(JSONValue.parse(sent) == JSONValue.parse(#"{"containerId":"c1","afterSequence":120,"pinned":false,"first":50}"#), "variables sent: \(sent)")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anAggregateOperationReturnsItsFieldsAsAValue() async throws {
    let transport = try await Self.openOperationRig("containerMedia", document: "query containerMedia($containerId: ID!, $upToSequence: Int) { containerMedia(containerId: $containerId, upToSequence: $upToSequence) { visualCount audioCount rewardCount latestSequence } }", reply: #"{"data":{"containerMedia":{"visualCount":3,"audioCount":1,"rewardCount":0,"latestSequence":120}}}"#)

    let result = try await Operations.containerMediaLoad(containerId: "c1", upToSequence: 120)

    #expect(result.visualCount == 3)
    #expect(result.latestSequence == 120)
    let sent = transport.calls.first { $0.operationName == "containerMedia" }?.variablesJson ?? ""
    #expect(JSONValue.parse(sent) == JSONValue.parse(#"{"containerId":"c1","upToSequence":120}"#), "variables sent: \(sent)")
  }
}
