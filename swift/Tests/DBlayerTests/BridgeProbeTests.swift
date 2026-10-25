import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct BridgeProbeTests {
  @Test
  func aSearchPassesWholeWordsToTheCore() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    _ = try await Moment.create(text: "hello world")
    _ = try await Moment.create(text: "hel")
    let whole = Cursor(try await Query<Moment>().search("hel", wholeWords: true).live.stream())
    #expect(await whole.first("the row with the whole word") { $0.map(\.text) == ["hel"] } != nil)
  }

  @Test
  func writesReachTypedLiveRows() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let changes = Cursor(try await Moment.recent.live.stream())
    #expect(await changes.next()?.isEmpty == true)
    let moment = try await Moment.create(text: "m")
    #expect(await changes.next()?.first?.text == "m")
    _ = try await moment.update(text: "edited")
    #expect(await changes.next()?.first?.text == "edited")
    try await moment.setLocal("tone", .string("warm"))
    #expect(await changes.next()?.first?.tone == "warm")
    try await moment.setLocal("tone", .null)
    #expect(await changes.next()?.first?.tone == nil)
    try await moment.destroy()
    #expect(await changes.next()?.isEmpty == true)
  }

  @Test
  func aWarmQueryAnswersFromMemoryAndALiveReadLeavesItWarm() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    #expect(try await Moment.recent.cached() == nil)
    let moment = try await Moment.create(text: "m")
    try await Moment.recent.warm()
    let cached = try await Moment.recent.cached()
    let warm = try #require(cached)
    #expect(warm.value.map(\.id) == [moment.id] && warm.value.first?.text == "m")
    #expect(try await Moment.recent.cachedRow("ghost") == nil)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    #expect(try await Moment.recent.cached() == nil)
    _ = try await Moment.create(text: "n")
    #expect(try await Moment.recent.live.current().count == 1)
    #expect(try await Moment.recent.cached()?.value.first?.text == "n")
  }

  @MainActor
  @Test
  func anEntityFollowsAnEditAndLetsGoOfTheRecordWhenItIsDropped() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let first = try await Moment.create(text: "one")
    var entity: Entity<Moment>? = Moment.entity(first.id)
    await eventually(#"not (entity?.value == nil)"#) { !(entity?.value == nil) }
    #expect(entity?.value?.text == "one")

    _ = try await first.update(text: "edited")
    await eventually(#"not (entity?.value?.text != "edited")"#) { !(entity?.value?.text != "edited") }
    #expect(entity?.value?.text == "edited")

    #expect(try DB.subscriptionCensus().subscriptions == 1)
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

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func loadRequestsOnlyAWindowThatIsNotFreshAndKeepsNoSubscription() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let before = try DB.subscriptionCensus()

    try await Moment.feed.load()
    try await Moment.feed.load()

    #expect(transport.calls.count == 1)
    #expect(try DB.subscriptionCensus().subscriptions == before.subscriptions && DB.subscriptionCensus().observedQueries == before.observedQueries)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aFailedLoadThrowsTheTransportErrorAndMarksTheRelationFailed() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = LiveList(Moment.feed)

    do {
      try await list.refresh()
      Issue.record("the failed load did not throw")
    } catch is DbError {
    } catch {
      Issue.record("the failed load surfaced as \(error), not as a database error")
    }
    await eventually("the relation fails") { list.snapshot.phase == .failed }

    #expect(list.snapshot.phase == .failed)
    #expect(list.snapshot.errorKind != nil)
  }

  @MainActor
  @Test
  func feedLoadsFromTheTransportIntoLiveRowsAndReportsItsState() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"totalCount":9}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)

    let list = LiveList(Moment.feed)
    await eventually(#"not (list.rows.isEmpty || list.snapshot.loadedAt == nil)"#) { !(list.rows.isEmpty || list.snapshot.loadedAt == nil) }

    #expect(list.rows.first?.uuid == "a")
    #expect(transport.calls.first?.operationName == "MomentsFeed")
    #expect(list.snapshot.hasMore && list.snapshot.phase == .loaded)
    #expect(list.snapshot.total == 9)
    let entity = Moment.entity("1")
    await eventually(#"not (entity.value == nil)"#) { !(entity.value == nil) }
    #expect(entity.value?.uuid == "a")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func eachNewListCanRefreshAndAwaitItsOwnInitialRequest() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed { feed { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    for index in 0..<3 {
      if index == 2 {
        transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "rejected"))
      } else {
        transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false}}}}"#)
      }
      let list = LiveList(Moment.feed, refreshOnConnect: true)
      do {
        try await list.waitForInitialLoad()
        #expect(index < 2)
      } catch DbError.Http(let status, _) {
        #expect(index == 2 && status == 400)
      }
      #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == index + 1)
      await eventually("initial rows remain visible") { list.rows.first?.uuid == "a" }
    }
  }

  @MainActor
  @Test
  func aCreateWhoseMutationIsNotRegisteredFailsOnTheRecordAndNeedsAttention() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport())

    let local = try await Moment.create(text: "hello", media: nil)
    let entity = Moment.entity(local.id)
    await eventually(#"not (entity.value?.sync.state != .failed)"#) { !(entity.value?.sync.state != .failed) }

    #expect(entity.value?.sync.state == .failed)
    #expect(entity.value?.needsAttention == true)
  }

  @Test
  func confirmedIdWaitsForTheServerIdOfALocalCreate() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)

    let local = try await Moment.create(text: "hello", media: nil)
    #expect(local.id.hasPrefix("temp"))
    try await local.settled(action: nil)
    let real = try await local.confirmedId()
    #expect(real == "m77")
  }

  @Test(.timeLimit(.minutes(1)))
  func aCreateSendsItsArgsAndTheRowNeverStoresThem() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt } }"}]"#)

    let local = try await Record.create(containerId: "c", authorId: "u1", text: "hi", attachedUserId: "42", signedId: "blob-1")
    try await local.settled(action: nil)
    let sent = transport.calls.first { $0.operationName == "CreateRecord" }?.variablesJson ?? ""
    #expect(sent.contains(#""signedId":"blob-1""#) && sent.contains(#""attachedUserId":42"#), "variables sent: \(sent)")
    #expect(try await Record.find(local.confirmedId()).current()?.text == "hi")
  }

  @Test(.timeLimit(.minutes(1)))
  func aSnapshotCarriesTheTemporaryIdThatTheServerIdReplaced() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    let snapshots = Cursor(try await Moment.recent.snapshots.stream())
    _ = await snapshots.next()

    let local = try await Moment.create(text: "hello", media: nil)
    let replaced = await snapshots.first("the snapshot that carries the replaced id") { $0.replaced["m77"] != nil }?.replaced["m77"]

    #expect(replaced == local.id)
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

    let list = LiveList(Moment.recent, snapshots: snapshots)
    await eventually("the replacement reaches the list") { !list.replaced.isEmpty }

    #expect(seen.mainThread == [false])
  }

  @Test
  func aCreateWithAnUploadShowsTheRecordAtOnceAndTheServerIdReplacesIt() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}"#)
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob($filename: String!, $contentType: String!, $byteSize: Int!, $checksum: String!) { createBlob(filename: $filename, contentType: $contentType, byteSize: $byteSize, checksum: $checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#, uploads: UploadOperations(createBlob: UploadOperation("CreateBlob")))
    let file = FileManager.default.temporaryDirectory.appending(path: "clip-\(UUID().uuidString).mp4")
    try Data("abcdef".utf8).write(to: file)

    let local = try await Moment.create(values: ["text": .string("hello")], action: "upload_clip", uploading: UploadRequest(path: file.path, variables: ["text": .string("hello")]))

    #expect(local.id.hasPrefix("temp"))
    #expect(local.uploads["upload_clip"] != nil)
    #expect(try await local.confirmedId() == "m77")
    let sent = transport.calls.first { $0.operationName == "CreateMoment" }
    #expect(sent?.variablesJson.contains("\"signedId\":\"sid\"") == true)
    #expect(sent?.variablesJson.contains("blobId") == false)
    #expect(transport.calls.filter { $0.operationName == "CreateMoment" }.count == 1)
  }

  @Test
  func aReactionUsesItsDeclaredKeyAcrossGeneratedValuesAndCells() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let initial = try await Record.create(containerId: "c1", authorId: "u1", text: "hello")
    let row = try await initial.update(reactions: [RecordReactionNode(body: "A", userId: "u1")])
    let changed = try await row.react(reactionBody: "C", userId: "u1")
    #expect(changed.reactions.map(\.userId) == ["u1"])
    #expect(changed.reactions.map(\.body) == ["C"])
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func resumeReachesTheCoreAndReloadsAWatchedRelationWithoutAFreshWindow() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = LiveList(Moment.feed)
    await eventually("the relation fails") { list.snapshot.phase == .failed }
    let before = transport.calls.count

    try await DB.resume()

    #expect(transport.calls.count > before)
  }

  private func openWithAMoment(_ transport: MockTransport) async throws -> Moment {
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"},{"name":"ShareMoment","document":"mutation ShareMoment($momentId: ID!) { shareMoment(momentId: $momentId) { moment { id uuid likedByMe media createdAt } successMessage copies } }"}]"#)
    let local = try await Moment.create(text: "hello", media: nil)
    try await local.settled(action: nil)
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
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let first = try await Moment.create(text: "one")
    let second = try await Moment.create(text: "two")
    let collector = EntityCollector()
    let entities = try DB.database.liveEntities(model: Moment.modelName, listener: collector)
    try entities.retain(ids: [first.id, second.id])
    #expect(await collector.wait(for: 1).first?.rows.count == 2)

    _ = try await first.update(text: "edited")
    let changed = await collector.wait(for: 2)
    #expect(changed.count == 2 && changed[1].rows.count == 1 && changed[1].rows[0].cells.contains(.text(value: "edited")))

    try entities.release(ids: [first.id])
    _ = try await first.update(text: "unseen")
    _ = try await second.update(text: "seen")
    let last = await collector.wait(for: 3)
    #expect(last.count == 3 && last[2].rows.count == 1 && last[2].rows[0].cells.contains(.text(value: "seen")))

    try await second.destroy()
    let removed = await collector.wait(for: 4)
    #expect(removed.last?.removed == [second.id])
  }

  /// REL-24: a reset of the account empties the entities that readers hold and the same objects receive the next rows.
  @MainActor
  @Test
  func aResetEmptiesTheHeldEntityAndTheSameObjectReceivesTheNextRow() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let moment = try await Moment.create(text: "old")
    let held = Moment.entity(moment.id)
    await eventually(#"held.value?.text == "old""#) { held.value?.text == "old" }

    try await DB.logout()
    await eventually(#"held.value == nil"#) { held.value == nil }
    #expect(!held.isClosed)
    _ = try await DB.database.create(model: "Moment", valuesJson: #"{"id":"\#(moment.id)","text":"new"}"#, argsJson: "{}")
    await eventually(#"held.value?.text == "new""#) { held.value?.text == "new" }
    #expect(held === Moment.entity(moment.id))
  }

  /// REL-25: closing the database closes the held entities and the next open starts empty.
  @MainActor
  @Test
  func aClosedDatabaseClosesTheHeldEntitiesAndTheNextOpenStartsEmpty() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let moment = try await Moment.create(text: "old")
    let held = Moment.entity(moment.id)
    await eventually(#"held.value?.text == "old""#) { held.value?.text == "old" }

    DB.close()
    #expect(held.isClosed && held.value == nil)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let fresh = Moment.entity(moment.id)
    let other = Moment.entity(try await Moment.create(text: "other").id)
    await eventually("a delivery of the new database reaches the registry") { other.value != nil }
    #expect(fresh.value == nil && fresh !== held && !fresh.isClosed && held.isClosed)
  }
}
