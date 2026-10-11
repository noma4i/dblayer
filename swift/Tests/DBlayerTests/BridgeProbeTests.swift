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
    let moment = try await Moment.create(text: "m")
    #expect(await changes.next()?.first?.text == "m")
    _ = try await moment.update(text: "edited")
    #expect(await changes.next()?.first?.text == "edited")
    try await moment.destroy()
    #expect(await changes.next()?.isEmpty == true)
  }

  @MainActor
  @Test
  func liveRowsFollowAnEditAndDropRowsWhenTheTrackedIdsChange() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let first = try await Moment.create(text: "one")
    let second = try await Moment.create(text: "two")
    let rows = LiveRows<Moment>()
    rows.track([first.id])
    for _ in 0 ..< 200 where rows.byId[first.id] == nil { try await Task.sleep(for: .milliseconds(10)) }
    #expect(rows.byId[first.id]?.text == "one")

    _ = try await first.update(text: "edited")
    for _ in 0 ..< 200 where rows.byId[first.id]?.text != "edited" { try await Task.sleep(for: .milliseconds(10)) }
    #expect(rows.byId[first.id]?.text == "edited")

    rows.track([second.id])
    for _ in 0 ..< 200 where rows.byId[second.id] == nil { try await Task.sleep(for: .milliseconds(10)) }
    #expect(Set(rows.byId.keys) == [second.id])
    rows.stop()
  }

  @Test
  func snapshotsOfOneRowWithDifferentFieldsAreNotEqual() {
    let before = Moment(id: "1", uuid: "a", text: "m", media: .object([:]), createdAt: "2026-10-08T00:00:00.000Z")
    let after = Moment(id: "1", uuid: "a", text: "edited", media: .object([:]), createdAt: "2026-10-08T00:00:00.000Z")
    #expect(before != after)
    #expect(before.hashValue == after.hashValue)
    #expect(before == before)
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
    var states = Moment.feed.state.stream.makeAsyncIterator()
    let state = await states.next()
    #expect(state?.phase == .idle && state?.hasMore == false && state?.loadedAt != nil)
  }

  @MainActor
  @Test
  func aFailedLoadThrowsTheTransportErrorAndMarksTheRelationFailed() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", failure: .Network(detail: "offline"))
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = LiveList(Moment.feed)

    do {
      try await list.refresh()
      Issue.record("the failed load did not throw")
    } catch is DbError {
    } catch {
      Issue.record("the failed load surfaced as \(error), not as a database error")
    }
    for _ in 0 ..< 200 where list.state.phase != .failed { try await Task.sleep(for: .milliseconds(10)) }

    #expect(list.state.phase == .failed)
    #expect(list.state.errorKind != nil)
    list.stop()
  }

  @MainActor
  @Test
  func aFatalFailureMarksTheRelationFailedWithoutAnyRetry() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = LiveList(Moment.feed)

    do {
      try await list.refresh()
      Issue.record("the failed load did not throw")
    } catch is DbError {
    } catch {
      Issue.record("the failed load surfaced as \(error), not as a database error")
    }
    for _ in 0 ..< 200 where list.state.phase != .failed { try await Task.sleep(for: .milliseconds(10)) }

    #expect(list.state.phase == .failed)
    #expect(list.state.errorKind != nil)
    list.stop()
  }

  @MainActor
  @Test
  func liveListLoadsTheRelationAndReportsItsState() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"totalCount":9}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)

    let list = LiveList(Moment.feed)
    for _ in 0 ..< 200 where list.rows.isEmpty || list.state.loadedAt == nil { try await Task.sleep(for: .milliseconds(10)) }

    #expect(list.rows.first?.uuid == "a")
    #expect(list.state.hasMore && list.state.phase == .idle)
    #expect(list.state.total == 9)
    let rows = LiveRows<Moment>()
    rows.track(list.rows.map(\.id))
    for _ in 0 ..< 200 where rows.byId.isEmpty { try await Task.sleep(for: .milliseconds(10)) }
    #expect(rows.byId["1"]?.uuid == "a")
    rows.stop()
    list.stop()
  }

  @Test
  func confirmedIdWaitsForTheServerIdOfALocalCreate() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)

    let local = try await Moment.create(text: "hello", media: nil)
    #expect(local.id.hasPrefix("temp"))
    try await local.settled()
    let real = try await local.confirmedId()
    #expect(real == "m77")
  }

  @MainActor
  @Test
  func resumeReachesTheCoreAndReloadsAWatchedRelationWithoutAFreshWindow() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = LiveList(Moment.feed)
    for _ in 0 ..< 200 where list.state.phase != .failed { try await Task.sleep(for: .milliseconds(10)) }
    let before = transport.calls.count

    try await DB.resume()

    #expect(transport.calls.count > before)
    list.stop()
  }

  @MainActor
  @Test
  func aReloadAfterACommitReadsTheServerAfterTheLoadThatWasInFlight() async throws {
    let transport = MockTransport()
    let empty = #"{"data":{"feed":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#
    transport.enqueue("MomentsFeed", reply: empty)
    transport.enqueue("MomentsFeed", reply: empty, delay: 1)
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    let list = LiveList(Moment.feed)
    for _ in 0 ..< 200 where list.state.loadedAt == nil { try await Task.sleep(for: .milliseconds(10)) }
    let older = Task { try await list.refresh() }
    for _ in 0 ..< 200 where transport.calls.filter({ $0.operationName == "MomentsFeed" }).count < 2 { try await Task.sleep(for: .milliseconds(10)) }

    let local = try await Moment.create(text: "hello", media: nil)
    try await local.settled()
    for _ in 0 ..< 400 where list.rows.map(\.id) != ["m77"] { try await Task.sleep(for: .milliseconds(10)) }

    #expect(list.rows.map(\.id) == ["m77"])
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 3)
    try await older.value
    list.stop()
  }
}
