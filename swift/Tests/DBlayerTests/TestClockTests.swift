import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aTestClockMovesFreshForWithoutSleeping() async throws {
    try await run(advance: 5 * 60 + 1, requests: 2)
    try await run(advance: 5 * 60 - 1, requests: 1)
  }

  @MainActor
  private func run(advance: TimeInterval, requests: Int) async throws {
    DB.close()
    let transport = MockTransport()
    for _ in 0 ..< 2 { transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#) }
    let clock = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)

    try await Moment.feed.load()
    clock.advance(by: advance)
    try await Moment.feed.load()

    #expect(transport.calls.count == requests)
  }
}
