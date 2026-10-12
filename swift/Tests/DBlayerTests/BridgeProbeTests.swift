import DBlayerTesting
import Foundation
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

  @MainActor
  @Test
  func aCreateWhoseMutationIsNotRegisteredFailsOnTheRecordAndSendsAfterRegisteringAndRetrying() async throws {
    let transport = MockTransport()
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)

    let local = try await Moment.create(text: "hello", media: nil)
    let rows = LiveRows<Moment>()
    rows.track([local.id])
    for _ in 0 ..< 200 where rows.byId[local.id]?.sync.state != .failed { try await Task.sleep(for: .milliseconds(10)) }
    let failed = rows.byId[local.id]
    #expect(failed?.sync.state == .failed)
    #expect(failed?.needsAttention == true)
    #expect(failed?.sync.error?.message == "operation CreateMoment is not registered")
    #expect(transport.calls.isEmpty)

    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    try await DB.database.retry(model: "Moment", rowId: local.id)
    #expect(try await local.confirmedId() == "m77")
    rows.stop()
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
    var snapshots = Moment.recent.snapshots.stream.makeAsyncIterator()
    _ = await snapshots.next()

    let local = try await Moment.create(text: "hello", media: nil)
    var replaced: String?
    while replaced == nil, let snapshot = await snapshots.next() {
      replaced = snapshot.replaced["m77"]
    }

    #expect(replaced == local.id)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aListFollowsTheRowsThatItsRowsRefer() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let list = LiveList(Chat.recent)
    let lastMessages = list.refs(Message.self) { $0.lastMessageId.map { [$0] } ?? [] }
    #expect(lastMessages.byId.isEmpty)

    let message = try await Message.create(chatId: "c", authorId: "u1", text: "hi")
    let chat = try await Chat.create(title: "t", lastMessageId: message.id)
    for _ in 0 ..< 100 where lastMessages.byId[message.id] == nil { try await Task.sleep(for: .milliseconds(20)) }

    #expect(lastMessages.byId[message.id]?.text == "hi")
    try await chat.destroy()
    for _ in 0 ..< 100 where lastMessages.byId[message.id] != nil { try await Task.sleep(for: .milliseconds(20)) }
    #expect(lastMessages.byId.isEmpty)
    list.stop()
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
    while list.replaced.isEmpty { await Task.yield() }

    #expect(seen.mainThread == [false, false])
    list.stop()
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
    var attempts = 0
    while await Moment.find(moment.id).current()?.likedByMe != false, attempts < 400 {
      attempts += 1
      try await Task.sleep(for: .milliseconds(10))
    }
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
    var attempts = 0
    while transport.calls.filter({ $0.operationName == "ShareMoment" }).count < 2, attempts < 400 {
      attempts += 1
      try await Task.sleep(for: .milliseconds(10))
    }
    try await Task.sleep(for: .milliseconds(100))
    await #expect(throws: (any Error).self) { try await refused.answer() }
  }
}

final class EntityCollector: EntityListener, @unchecked Sendable {
  private let lock = NSLock()
  private var received: [EntityDelivery] = []
  func changed(delivery: EntityDelivery) { lock.withLock { received.append(delivery) } }
  var deliveries: [EntityDelivery] { lock.withLock { received } }
  func wait(for count: Int) async throws -> [EntityDelivery] {
    for _ in 0 ..< 300 where deliveries.count < count { try await Task.sleep(for: .milliseconds(10)) }
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
    #expect(try await collector.wait(for: 1).first?.rows.count == 2)

    _ = try await first.update(text: "edited")
    let changed = try await collector.wait(for: 2)
    #expect(changed.count == 2 && changed[1].rows.count == 1 && changed[1].rows[0].contains("edited"))

    try entities.release(ids: [first.id])
    _ = try await first.update(text: "unseen")
    _ = try await second.update(text: "seen")
    let last = try await collector.wait(for: 3)
    #expect(last.count == 3 && last[2].rows.count == 1 && last[2].rows[0].contains("seen"))

    try await second.destroy()
    let removed = try await collector.wait(for: 4)
    #expect(removed.last?.removed == [second.id])
  }
}
