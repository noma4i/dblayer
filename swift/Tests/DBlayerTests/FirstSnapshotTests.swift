import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let feedOperations = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theFirstValueOfAWindowLoadedByAnEarlierRunHoldsItsRowsAndTheLoadedPhase() async throws {
    let path = FileManager.default.temporaryDirectory.appending(path: "first-snapshot-\(UUID().uuidString).sqlite").path
    defer { for suffix in ["", "-wal", "-shm"] { try? FileManager.default.removeItem(atPath: path + suffix) } }
    let config = Config(path: path, dataVersion: nil, cacheRows: 32)
    let earlier = MockTransport()
    earlier.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    try await reopen(config: config, modelsJson: modelsJSON, transport: earlier)
    try DB.database.registerOperations(defsJson: Self.feedOperations)
    try await Moment.feed.load()

    try await reopen(config: config, modelsJson: modelsJSON, transport: MockTransport())
    try DB.database.registerOperations(defsJson: Self.feedOperations)
    let values = Cursor(try await Moment.feed.snapshots.stream())
    let first = try #require(await values.next())

    #expect(first.phase == .loaded)
    #expect(first.rows.map(\.uuid) == ["a"])
    #expect(first.loadedAt != nil)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aListThatCannotAttachShowsTheFailureInItsSnapshot() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: nil)
    DB.close()

    let list = LiveList(Moment.recent)
    await eventually("the attach failure reaches the snapshot") { list.snapshot.phase == .failed }

    #expect(list.snapshot.errorKind == "subscription")
    #expect(list.snapshot.error != nil)
  }
}
