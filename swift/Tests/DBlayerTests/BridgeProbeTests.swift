import DBlayerTesting
import Testing
@testable import DBlayer

@Suite(.serialized)
struct BridgeProbeTests {
  @Test
  func writesReachTypedLiveRows() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    var changes = Moment.recent.live.stream.makeAsyncIterator()
    #expect(await changes.next()?.isEmpty == true)
    let moment = try await Moment.create(uuid: "m", createdAt: "2026-10-08T00:00:00.000Z")
    #expect(await changes.next()?.first?.uuid == "m")
    _ = try await moment.update(text: "edited")
    #expect(await changes.next()?.first?.text == "edited")
    try await moment.destroy()
    #expect(await changes.next()?.isEmpty == true)
  }

  @Test
  func feedLoadsFromTheTransportIntoLiveRows() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)

    try await Moment.feed.refresh()

    var changes = Moment.feed.live.stream.makeAsyncIterator()
    #expect(await changes.next()?.first?.uuid == "a")
    #expect(transport.calls.first?.operationName == "MomentsFeed")
  }
}
