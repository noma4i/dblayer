import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func cancellingOneLoadInterestKeepsTheOtherCallAlive() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    let hold = transport.hold("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    defer { hold.release() }
    try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let (signals, signal) = AsyncStream<Int>.makeStream()
    let cursor = Cursor(signals)
    defer { signal.finish() }
    let first = try Moment.feed.beginLoad()
    let entered = Task { await hold.entered(); signal.yield(1) }
    defer { entered.cancel() }
    guard await cursor.next("the shared load starts") == 1 else { Issue.record("the load did not start"); return }
    let second = try Moment.feed.beginLoad()
    defer { first.cancel(); second.cancel() }
    first.cancel()
    hold.release()
    let completed = Task { do { try await second.result(); signal.yield(2) } catch { signal.yield(9) } }
    defer { completed.cancel() }
    guard await cursor.next("the second load finishes") == 2 else { Issue.record("the second load failed"); return }
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 1)
    #expect(try await Moment.feed.read().map(\.id) == ["1"])
  }

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

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aRefreshAfterReopenAndClockRollbackKeepsTheNewResponse() async throws {
    await DB.closeForCleanup()
    let path = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString).path
    defer { Task { @MainActor in try? await DB.close(); try? FileManager.default.removeItem(atPath: path) } }
    let transport = MockTransport()
    transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"A"}}}"#)
    transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"B"}}}"#)
    let definitions = #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]"#
    try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: definitions, path: path, startingAt: Date(timeIntervalSince1970: 1_000))
    let (events, signal) = AsyncStream<Int>.makeStream()
    let cursor = Cursor(events)
    defer { signal.finish() }
    let firstRefresh = Task { do { try await CurrentUser.current.refresh(); signal.yield(1) } catch { signal.yield(9) } }
    defer { firstRefresh.cancel() }
    guard await cursor.next("the first refresh finishes") == 1 else { Issue.record("the first refresh failed"); return }
    let first = Projected<CurrentUser>(try DB.account().stream(CurrentUser.current))
    guard await observe("the initial profile", { first.value != nil }) else { return }
    try #require(first.value?.name == "A")
    await first.stream.cancel()
    await DB.closeForCleanup()

    try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: definitions, path: path, startingAt: Date(timeIntervalSince1970: 1))
    let secondRefresh = Task { do { try await CurrentUser.current.refresh(); signal.yield(2) } catch { signal.yield(9) } }
    defer { secondRefresh.cancel() }
    guard await cursor.next("the reopened refresh finishes") == 2 else { Issue.record("the reopened refresh failed"); return }
    let second = Projected<CurrentUser>(try DB.account().stream(CurrentUser.current))
    guard await observe("the reopened profile", { second.value != nil }) else { return }
    #expect(second.value?.name == "B")
    #expect(transport.calls.filter { $0.operationName == "CurrentUserQuery" }.count == 2)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aTestClockMovesFreshForWithoutSleeping() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    for _ in 0 ..< 2 { transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#) }
    let down = transport.hold("MomentsFeed", failure: .Network(detail: "down"))
    let clock = try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)

    try await Moment.feed.load()
    let held = Projected<Moment>(try DB.account().stream(Moment.feed))
    try await held.loaded()
    clock.advance(by: 5 * 60 - 1)
    try await Moment.feed.load()
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 1)
    clock.advance(by: 1)
    try await Moment.feed.load()
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 2)
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    clock.advance(by: 5 * 60)
    let failing = Task { try await Moment.feed.load() }
    let finished = Done()
    let watcher = Task { _ = await failing.result; finished.set() }
    defer { watcher.cancel() }
    try await networkDrops(during: down)
    down.release()
    await eventually("the third request answered") { transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 3 }
    #expect(!finished.value, "a load without network keeps waiting")
    try DB.database.setOnline(online: true)
    try await valueBeforeDeadline(failing) {}
    guard await observe("the window shows the loaded rows", { held.phase == .loaded && held.error == nil }) else { return }
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 4, "the load that waited for the network was read once more, not once per cause")
    #expect(held.rows.map(\.id) == ["1"])
  }
}
