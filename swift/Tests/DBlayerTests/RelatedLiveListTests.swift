import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aLiveListOverARelatedQueryLoadsItsSourceOnceWhenStaleAndStaysLiveWhileRead() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let before = try DB.subscriptionCensus().observedQueries
    let authors: RelatedQuery<User> = Moment.feed.related(Moment.Relations.author)

    let first = LiveList(authors)
    try await first.waitForInitialLoad()
    await eventually("the source scope is loaded") { first.snapshot.loadedAt != nil }
    #expect(transport.calls.count == 1)
    #expect(try DB.subscriptionCensus().observedQueries > before)

    let second = LiveList(authors)
    try await second.waitForInitialLoad()
    await eventually("the second list shows the fresh source") { second.snapshot.loadedAt != nil }
    #expect(transport.calls.count == 1)
  }
}
