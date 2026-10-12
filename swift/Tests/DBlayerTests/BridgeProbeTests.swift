import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct BridgeProbeTests {
  /// Waits for the event that the relation failed, not for a time.
  static func failedState(of query: Query<Moment>) async -> LoadState {
    await Cursor(await query.state.stream()).first("the relation to fail") { $0.phase == .failed } ?? LoadState()
  }

  @Test
  func writesReachTypedLiveRows() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let changes = Cursor(await Moment.recent.live.stream())
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
  func anEntityFollowsAnEditAndLetsGoOfTheRecordWhenItIsDropped() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let first = try await Moment.create(text: "one")
    var entity: Entity<Moment>? = Moment.entity(first.id)
    await eventually(#"not (entity?.value == nil)"#) { !(entity?.value == nil) }
    #expect(entity?.value?.text == "one")

    _ = try await first.update(text: "edited")
    await eventually(#"not (entity?.value?.text != "edited")"#) { !(entity?.value?.text != "edited") }
    #expect(entity?.value?.text == "edited")

    #expect(DB.subscriptionCensus().subscriptions == 1)
    entity = nil
    await eventually(#"not (Entities<Moment>.shared.liveCount > 0)"#) { !(Entities<Moment>.shared.liveCount > 0) }
    #expect(Entities<Moment>.shared.liveCount == 0)
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

    let changes = Cursor(await Moment.feed.live.stream())
    #expect(await changes.next()?.first?.uuid == "a")
    #expect(transport.calls.first?.operationName == "MomentsFeed")
    let states = Cursor(await Moment.feed.state.stream())
    let state = await states.next()
    #expect(state?.phase == .idle && state?.hasMore == false && state?.loadedAt != nil)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func loadRequestsOnlyAWindowThatIsNotFreshAndKeepsNoSubscription() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let before = DB.subscriptionCensus()

    try await Moment.feed.load()
    try await Moment.feed.load()

    #expect(transport.calls.count == 1)
    #expect(DB.subscriptionCensus().subscriptions == before.subscriptions && DB.subscriptionCensus().observedQueries == before.observedQueries)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aFailedLoadThrowsLikeRefresh() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", failure: .Network(detail: "offline"))
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)

    do {
      try await Moment.feed.load()
      Issue.record("the failed load did not throw")
    } catch is DbError {}
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
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
    let failed = await Self.failedState(of: Moment.feed)

    #expect(failed.phase == .failed)
    #expect(failed.errorKind != nil)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
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
    let failed = await Self.failedState(of: Moment.feed)

    #expect(failed.phase == .failed)
    #expect(failed.errorKind != nil)
  }

  @MainActor
  @Test
  func liveListLoadsTheRelationAndReportsItsState() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"totalCount":9}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)

    let list = LiveList(Moment.feed)
    await eventually(#"not (list.rows.isEmpty || list.state.loadedAt == nil)"#) { !(list.rows.isEmpty || list.state.loadedAt == nil) }

    #expect(list.rows.first?.uuid == "a")
    #expect(list.state.hasMore && list.state.phase == .idle)
    #expect(list.state.total == 9)
    let entity = Moment.entity("1")
    await eventually(#"not (entity.value == nil)"#) { !(entity.value == nil) }
    #expect(entity.value?.uuid == "a")
  }

  @MainActor
  @Test
  func aCreateWhoseMutationIsNotRegisteredFailsOnTheRecordAndSendsAfterRegisteringAndRetrying() async throws {
    let transport = MockTransport()
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)

    let local = try await Moment.create(text: "hello", media: nil)
    let entity = Moment.entity(local.id)
    await eventually(#"not (entity.value?.sync.state != .failed)"#) { !(entity.value?.sync.state != .failed) }
    let failed = entity.value
    #expect(failed?.sync.state == .failed)
    #expect(failed?.needsAttention == true)
    #expect(failed?.sync.error?.message == "operation CreateMoment is not registered")
    #expect(transport.calls.isEmpty)

    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    try await DB.database.retry(model: "Moment", rowId: local.id)
    #expect(try await local.confirmedId() == "m77")
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

  @Test(.timeLimit(.minutes(1)))
  func aSnapshotCarriesTheTemporaryIdThatTheServerIdReplaced() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    let snapshots = Cursor(await Moment.recent.snapshots.stream())
    _ = await snapshots.next()

    let local = try await Moment.create(text: "hello", media: nil)
    let replaced = await snapshots.first("the snapshot that carries the replaced id") { $0.replaced["m77"] != nil }?.replaced["m77"]

    #expect(replaced == local.id)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anEntityOfARecordThatAnotherRecordRefersToFollowsItsCreateAndDestroy() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let message = try await Message.create(chatId: "c", authorId: "u1", text: "hi")
    let entity = Message.entity(message.id)
    await eventually(#"not (entity.value == nil)"#) { !(entity.value == nil) }
    #expect(entity.value?.text == "hi")
    try await message.destroy()
    await eventually(#"not (entity.value != nil)"#) { !(entity.value != nil) }
    #expect(entity.value == nil)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aLiveListAttachesItsStreamsOffTheMainThread() async throws {
    final class Seen: @unchecked Sendable {
      let lock = NSLock()
      var mainThread: [Bool] = []
      func note() { lock.lock(); mainThread.append(Thread.isMainThread); lock.unlock() }
    }
    let seen = Seen()
    let snapshots = LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:])) { emit in
      seen.note()
      emit(Snapshot(rows: [], replaced: ["a": "b"]))
      return {}
    }
    let state = LiveSource<LoadState>(initial: LoadState()) { _ in
      seen.note()
      return {}
    }

    let list = LiveList(Moment.recent, snapshots: snapshots, state: state)
    await eventually("the replacement reaches the list") { !list.replaced.isEmpty }

    #expect(seen.mainThread == [false, false])
  }

  @Test
  func aCreateWithAnUploadShowsTheRecordAtOnceAndTheServerIdReplacesIt() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}"#)
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateBlob","document":"mutation CreateBlob($filename: String!, $contentType: String!, $byteSize: Int!, $checksum: String!) { createBlob(filename: $filename, contentType: $contentType, byteSize: $byteSize, checksum: $checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    let file = FileManager.default.temporaryDirectory.appending(path: "clip-\(UUID().uuidString).mp4")
    try Data("abcdef".utf8).write(to: file)

    let local = try await Moment.create(values: ["text": .string("hello")], uploading: UploadRequest(path: file.path, mutation: "CreateMoment", createBlob: "CreateBlob", variables: ["text": .string("hello")]))

    #expect(local.id.hasPrefix("temp"))
    #expect(local.upload != nil)
    #expect(try await local.confirmedId() == "m77")
    let sent = transport.calls.first { $0.operationName == "CreateMoment" }
    #expect(sent?.variablesJson.contains("\"signedId\":\"sid\"") == true)
    #expect(sent?.variablesJson.contains("blobId") == false)
    #expect(transport.calls.filter { $0.operationName == "CreateMoment" }.count == 1)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func resumeReachesTheCoreAndReloadsAWatchedRelationWithoutAFreshWindow() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = LiveList(Moment.feed)
    _ = await Self.failedState(of: Moment.feed)
    let before = transport.calls.count

    try await DB.resume()

    #expect(transport.calls.count > before)
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
    await eventually(#"not (list.state.loadedAt == nil)"#) { !(list.state.loadedAt == nil) }
    let older = Task { try await list.refresh() }
    await eventually(#"not (transport.calls.filter({ $0.operationName == "MomentsFeed" }).count < 2)"#) { !(transport.calls.filter({ $0.operationName == "MomentsFeed" }).count < 2) }

    let local = try await Moment.create(text: "hello", media: nil)
    try await local.settled()
    await eventually(#"not (list.rows.map(\.id) != ["m77"])"#) { !(list.rows.map(\.id) != ["m77"]) }

    #expect(list.rows.map(\.id) == ["m77"])
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 3)
    try await older.value
  }

  @Test
  func aRefusalAnsweredBeforeSettledStartedIsStillThrownOnce() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"},{"name":"LikeMoment","document":"mutation LikeMoment($momentId: ID!) { likeMoment(momentId: $momentId) { id uuid likedByMe media createdAt } }"}]"#)
    let local = try await Moment.create(text: "hello", media: nil)
    try await local.settled()
    let moment = try #require(await Moment.find(try await local.confirmedId()).current())

    transport.enqueue("LikeMoment", reply: #"{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}"#)
    let liked = try await moment.like()
    #expect(liked.likedByMe)
    await eventually("the refused like rolls back") { await Moment.find(moment.id).current()?.likedByMe == false }
    #expect(await Moment.find(moment.id).current()?.likedByMe == false)

    await #expect(throws: (any Error).self) { try await liked.settled() }
    try await liked.settled()
  }

  private func openWithAMoment(_ transport: MockTransport) async throws -> Moment {
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"},{"name":"ShareMoment","document":"mutation ShareMoment($momentId: ID!) { shareMoment(momentId: $momentId) { moment { id uuid likedByMe media createdAt } successMessage copies } }"}]"#)
    let local = try await Moment.create(text: "hello", media: nil)
    try await local.settled()
    return try #require(await Moment.find(try await local.confirmedId()).current())
  }

  @Test
  func anActionAnswersWithTheDeclaredFieldsOfThePayloadAndRefusesWhenTheServerRefuses() async throws {
    let transport = MockTransport()
    let moment = try await openWithAMoment(transport)

    transport.enqueue("ShareMoment", reply: #"{"data":{"shareMoment":{"moment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"},"successMessage":"Shared","copies":3,"other":"x"}}}"#)
    let shared = try await moment.share()
    #expect(shared.model.id == "m77")
    let answer = try await shared.answer()
    #expect(answer.successMessage == "Shared")
    #expect(answer.copies == 3)
    await #expect(throws: (any Error).self) { try await shared.answer() }

    transport.enqueue("ShareMoment", reply: #"{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}"#)
    let refused = try await moment.share()
    await eventually("the second ShareMoment call") { transport.calls.filter { $0.operationName == "ShareMoment" }.count >= 2 }
    try await Task.sleep(for: .milliseconds(100))
    await #expect(throws: (any Error).self) { try await refused.answer() }
  }
}

final class EntityCollector: EntityListener, @unchecked Sendable {
  private let lock = NSLock()
  private var received: [EntityDelivery] = []
  func changed(delivery: EntityDelivery) { lock.withLock { received.append(delivery) } }
  var deliveries: [EntityDelivery] { lock.withLock { received } }
  func wait(for count: Int) async -> [EntityDelivery] {
    await eventually("\(count) entity deliveries") { deliveries.count >= count }
    return deliveries
  }
}

extension BridgeProbeTests {
  @Test
  func retainedRowsReachTheBridgeAsChangedRowsAndReleasedRowsStayQuiet() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let first = try await Moment.create(text: "one")
    let second = try await Moment.create(text: "two")
    let collector = EntityCollector()
    let entities = try DB.database.liveEntities(model: Moment.modelName, listener: collector)
    try entities.retain(ids: [first.id, second.id])
    #expect(await collector.wait(for: 1).first?.rows.count == 2)

    _ = try await first.update(text: "edited")
    let changed = await collector.wait(for: 2)
    #expect(changed.count == 2 && changed[1].rows.count == 1 && changed[1].rows[0].contains("edited"))

    try entities.release(ids: [first.id])
    _ = try await first.update(text: "unseen")
    _ = try await second.update(text: "seen")
    let last = await collector.wait(for: 3)
    #expect(last.count == 3 && last[2].rows.count == 1 && last[2].rows[0].contains("seen"))

    try await second.destroy()
    let removed = await collector.wait(for: 4)
    #expect(removed.last?.removed == [second.id])
  }

  /// REL-24: a newly opened database starts with no entity of the old one.
  @MainActor
  @Test
  func aNewDatabaseStartsWithNoEntityOfTheOldOne() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let moment = try await Moment.create(text: "old")
    let old = Moment.entity(moment.id)
    await eventually(#"not (old.value == nil)"#) { !(old.value == nil) }
    #expect(old.value?.text == "old")

    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let fresh = Moment.entity(moment.id)
    try await Task.sleep(for: .milliseconds(200))
    #expect(old.value == nil && fresh.value == nil && fresh !== old)
    let created = try await Moment.create(text: "new")
    let again = Moment.entity(created.id)
    await eventually(#"not (again.value == nil)"#) { !(again.value == nil) }
    #expect(again.value?.text == "new")
  }
}
