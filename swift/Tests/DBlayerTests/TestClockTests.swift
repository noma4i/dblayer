import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aMutationDuringTheFirstPageKeepsTheWholeChainStale() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    let row1 = #"{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}"#
    let row2 = #"{"__typename":"Moment","id":"2","uuid":"b","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}"#
    func page(_ rows: String, _ more: Bool, _ cursor: String) -> String {
      #"{"data":{"feed":{"nodes":[\#(rows)],"pageInfo":{"hasNextPage":\#(more),"endCursor":"\#(cursor)"}}}}"#
    }
    transport.enqueue("MomentsFeed", reply: page(row1, false, "seed"))
    let firstPage = transport.hold("MomentsFeed", reply: page(row1, true, "first"))
    transport.enqueue("MomentsFeed", reply: page(row2, false, "second"))
    transport.enqueue("MomentsFeed", reply: page(row2, false, "fresh"))
    transport.enqueue("LikeMoment", reply: #"{"data":{"likeMoment":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":true,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    let definitions = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"LikeMoment","document":"mutation LikeMoment($momentId: ID!) { likeMoment(momentId: $momentId) { id uuid likedByMe media createdAt } }"}]"#
    try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: definitions)
    let (signals, signal) = AsyncStream<Int>.makeStream()
    let cursor = Cursor(signals)
    defer { signal.finish(); firstPage.release() }
    let seed = Task { do { try await Moment.feed.load(); signal.yield(4) } catch { signal.yield(9) } }
    defer { seed.cancel() }
    guard await cursor.next("the seed load finishes") == 4 else { Issue.record("the seed load failed"); return }
    let row = try #require(try await Moment.feed.read().first)
    let loading = Task { do { try await Moment.feed.loadAll(force: true); signal.yield(2) } catch { signal.yield(9) } }
    defer { loading.cancel() }
    let entered = Task { await firstPage.entered(); signal.yield(1) }
    defer { entered.cancel() }
    guard await cursor.next("the first page is held") == 1 else { Issue.record("the first page did not start"); return }
    let mutation = Task {
      do {
        let receipt = try await row.like()
        if case .confirmed = try await receipt.settled() { signal.yield(3) } else { signal.yield(9) }
      } catch { signal.yield(9) }
    }
    defer { mutation.cancel() }
    guard await cursor.next("the mutation is confirmed") == 3 else { Issue.record("the mutation was not confirmed"); return }
    firstPage.release()
    guard await cursor.next("the old chain finishes") == 2 else { Issue.record("the chain did not finish"); return }
    try #require(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 3)
    let fresh = Task { do { try await Moment.feed.load(); signal.yield(5) } catch { signal.yield(9) } }
    defer { fresh.cancel() }
    guard await cursor.next("the fresh load finishes") == 5 else { Issue.record("the fresh load failed"); return }
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 4)
    #expect(try await Moment.feed.read().map(\.id) == ["2"])
    let repeated = Task { do { try await Moment.feed.load(); signal.yield(6) } catch { signal.yield(9) } }
    defer { repeated.cancel() }
    guard await cursor.next("the repeated load finishes") == 6 else { Issue.record("the repeated load failed"); return }
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 4)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aRefetchOwedWithoutAReaderSurvivesReopen() async throws {
    await DB.closeForCleanup()
    let path = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString).path
    defer { Task { @MainActor in try? await DB.close(); try? FileManager.default.removeItem(atPath: path) } }
    let transport = MockTransport()
    for (id, uuid) in [("1", "a"), ("2", "b")] {
      transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"\#(id)","uuid":"\#(uuid)","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    }
    transport.enqueue("LikeMoment", reply: #"{"data":{"likeMoment":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":true,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    let definitions = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"LikeMoment","document":"mutation LikeMoment($momentId: ID!) { likeMoment(momentId: $momentId) { id uuid likedByMe media createdAt } }"}]"#
    try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: definitions, path: path, startingAt: Date(timeIntervalSince1970: 1_000))
    let (events, signal) = AsyncStream<Int>.makeStream()
    let cursor = Cursor(events)
    defer { signal.finish() }
    let firstLoad = Task { do { try await Moment.feed.load(); signal.yield(1) } catch { signal.yield(9) } }
    defer { firstLoad.cancel() }
    guard await cursor.next("the first load finishes") == 1 else { Issue.record("the first load failed"); return }
    let before = try await Moment.feed.read()
    let row = try #require(before.first)
    try #require(row.id == "1")
    let outcome = Task {
      do {
        let receipt = try await row.like()
        if case .confirmed = try await receipt.settled() { signal.yield(2) } else { signal.yield(9) }
      }
      catch { signal.yield(9) }
    }
    defer { outcome.cancel() }
    guard await cursor.next("the mutation is confirmed") == 2 else { Issue.record("the mutation was not confirmed"); return }
    try #require(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 1)
    await DB.closeForCleanup()
    try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: definitions, path: path, startingAt: Date(timeIntervalSince1970: 1_000))
    let secondLoad = Task { do { try await Moment.feed.load(); signal.yield(3) } catch { signal.yield(9) } }
    defer { secondLoad.cancel() }
    guard await cursor.next("the reopened load finishes") == 3 else { Issue.record("the reopened load failed"); return }
    #expect(try await Moment.feed.read().map(\.id) == ["2"])
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 2)
  }
}
