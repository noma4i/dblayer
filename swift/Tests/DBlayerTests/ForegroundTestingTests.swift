import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private enum ForegroundFeed: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "MomentsFeed"
  static let document = "query MomentsFeed { feed { nodes { id uuid createdAt } pageInfo { hasNextPage endCursor } } }"
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func inv_api_07_aTestingForegroundEventReloadsOnlyAWindowWithAReader() async throws {
    DB.close()
    defer { DB.close() }
    var models = try #require(JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as? [[String: Any]])
    let index = try #require(models.firstIndex { $0["key"] as? String == "Moment" })
    var relations = try #require(models[index]["relations"] as? [[String: Any]])
    let feed = try #require(relations.firstIndex { $0["name"] as? String == "feed" })
    relations[feed]["refresh_on_foreground"] = true
    models[index]["relations"] = relations
    let schema = String(decoding: try JSONSerialization.data(withJSONObject: models), as: UTF8.self)
    let transport = MockTransport()
    try await DB.openTesting(transport: transport, modelsJSON: schema, operations: [ForegroundFeed.self])
    try DB.enterForegroundForTesting()
    try await DB.resume()
    #expect(transport.calls.isEmpty)

    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"id":"m1","uuid":"first","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"id":"m2","uuid":"second","createdAt":"2026-10-27T00:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    let list = LiveList(Moment.feed)
    try await list.waitForInitialLoad()
    #expect(transport.calls.map(\.operationName) == ["MomentsFeed"])
    try DB.enterForegroundForTesting()
    try await DB.resume()
    await eventually("the refreshed row reaches the reader") { list.ids == ["m2"] }
    #expect(transport.calls.map(\.operationName) == ["MomentsFeed", "MomentsFeed"])
  }
}
