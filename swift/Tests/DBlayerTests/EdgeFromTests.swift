import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let renamedEdgeDocument = "query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { edges { impressionScore impressionUuid rank node { id uuid likedByMe media createdAt } } pageInfo { hasNextPage endCursor } } }"

  private static let renamedEdgeReply = #"{"data":{"feed":{"edges":[{"impressionUuid":"imp-1","impressionScore":9,"rank":"top","node":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z","text":"in a vibe"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c"}}}}"#

  @Test(.timeLimit(.minutes(1)))
  func anEdgeValueDeclaredWithFromAndAnEdgeEnumAreReadAfterTheLoad() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("MomentsFeed"), "document": .string(Self.renamedEdgeDocument)])]).text)
    transport.enqueue("MomentsFeed", reply: Self.renamedEdgeReply)
    try await Moment.vibeFeed("v1").refresh()
    let edge = try Moment.vibeFeedEdge("v1", of: "1")
    #expect(edge?.score == 9)
    #expect(edge?.rank == .top)
    #expect(edge?.impressionUuid == "imp-1")
  }
}
