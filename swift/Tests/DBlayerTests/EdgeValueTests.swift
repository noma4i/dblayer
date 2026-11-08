import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let edgeFeedDocument = "query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { edges { impressionScore impressionUuid rank node { id uuid likedByMe media createdAt } } pageInfo { hasNextPage endCursor } } }"

  private static func edgeReply(uuid: String?, score: Int?, text: String? = nil) -> String {
    let uuidText = uuid.map { #""\#($0)""# } ?? "null"
    let body = text.map { #","text":"\#($0)""# } ?? ""
    return #"{"data":{"feed":{"edges":[{"impressionUuid":\#(uuidText),"impressionScore":\#(score.map(String.init) ?? "null"),"node":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"\#(body)}}],"pageInfo":{"hasNextPage":false,"endCursor":"c"}}}}"#
  }

  private static let rankedEdgeReply = #"{"data":{"feed":{"edges":[{"impressionUuid":"imp-1","impressionScore":9,"rank":"top","node":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z","text":"in a vibe"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c"}}}}"#

  private func openEdgeRig() async throws -> MockTransport {
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("MomentsFeed"), "document": .string(Self.edgeFeedDocument)])]).text)
    return transport
  }

  @Test(.timeLimit(.minutes(1)))
  func edgeCallsDecodeNullableFieldsAliasesAndEnums() async throws {
    let transport = try await openEdgeRig()
    transport.enqueue("MomentsFeed", reply: Self.edgeReply(uuid: "imp-1", score: 5))
    try await Moment.feed.refresh()
    #expect(try await Moment.feedEdge(of: "1") == MomentFeedEdge(["impressionUuid": "imp-1", "impressionScore": 5]))
    #expect(try await Moment.feedEdge(of: "1")?.impressionUuid == "imp-1")
    #expect(try await Moment.feedEdge(of: "unknown") == nil)

    transport.enqueue("MomentsFeed", reply: Self.edgeReply(uuid: nil, score: nil, text: "in a vibe"))
    try await Moment.vibeFeed("v1").refresh()
    #expect(try await Moment.vibeFeedEdge("v1", of: "1") == MomentVibeFeedEdge([:]))

    transport.enqueue("MomentsFeed", reply: Self.rankedEdgeReply)
    try await Moment.vibeFeed("v1").refresh()
    let ranked = try await Moment.vibeFeedEdge("v1", of: "1")
    #expect(ranked?.score == 9 && ranked?.rank == .top && ranked?.impressionUuid == "imp-1")
  }
}
