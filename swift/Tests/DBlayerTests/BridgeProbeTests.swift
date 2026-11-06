import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct BridgeProbeTests {
  @Test
  func aSearchPassesWholeWordsToTheCore() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON)
    _ = try await Moment.send(in: DB.account(), text: "hello world").optimisticValue!
    _ = try await Moment.send(in: DB.account(), text: "hel").optimisticValue!
    let whole = Cursor(try await Query<Moment>().search("hel", wholeWords: true).live.stream())
    #expect(await whole.first("the row with the whole word") { $0.map(\.text) == ["hel"] } != nil)
  }

  @Test
  func writesReachTypedLiveRows() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON)
    let changes = Cursor(try await Moment.recent.live.stream())
    #expect(await changes.next()?.isEmpty == true)
    let moment = try await Moment.send(in: DB.account(), text: "m").optimisticValue!
    #expect(await changes.next()?.first?.text == "m")
    try await moment.setLocal("tone", .string("warm"))
    #expect(await changes.next()?.first?.tone == "warm")
    try await moment.setLocal("tone", .null)
    #expect(await changes.next()?.first?.tone == nil)
  }

  @Test @MainActor
  func updateAndDestroyReachTypedLiveRows() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","text":"m","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    transport.enqueue("UpdateMoment", reply: #"{"data":{"updateMoment":{"__typename":"Moment","id":"1","uuid":"a","text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    transport.enqueue("DeleteMoment", reply: #"{"data":{"deleteMoment":{"id":"1"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid text likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"UpdateMoment","document":"mutation UpdateMoment($id: ID!, $text: String) { updateMoment(id: $id, text: $text) { id uuid text likedByMe media createdAt } }"},{"name":"DeleteMoment","document":"mutation DeleteMoment($id: ID!) { deleteMoment(id: $id) { id } }"}]"#)
    let changes = Cursor(try await Moment.recent.live.stream())
    try await Moment.feed.load()
    let loaded = await changes.first("the loaded row") { $0.map(\.text) == ["m"] }
    let moment = try #require(loaded?.first)
    _ = try await moment.update(text: "edited")
    #expect(await changes.first("the edited row") { $0.map(\.text) == ["edited"] } != nil)
    try await moment.destroy()
    #expect(await changes.first("the empty list") { $0.isEmpty } != nil)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func loadRequestsOnlyAWindowThatIsNotFreshAndKeepsNoSubscription() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let before = try await DB.subscriptionCensus()

    try await Moment.feed.load()
    try await Moment.feed.load()

    #expect(transport.calls.count == 1)
    let after = try await DB.subscriptionCensus()
    #expect(after.subscriptions == before.subscriptions && after.observedQueries == before.observedQueries)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aFailedLoadThrowsTheTransportErrorAndMarksTheRelationFailed() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request"))
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = Projected<Moment>(try AccountScope().stream(Moment.feed))

    do {
      try await list.refresh()
      Issue.record("the failed load did not throw")
    } catch is DbError {
    } catch {
      Issue.record("the failed load surfaced as \(error), not as a database error")
    }
    await eventually("the relation fails") { list.phase == .failed }

    #expect(list.phase == .failed)
    #expect(list.state?.error != nil)
  }

  @MainActor
  @Test
  func feedLoadsFromTheTransportIntoLiveRowsAndReportsItsState() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"},"totalCount":9}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)

    let list = Projected<Moment>(try AccountScope().stream(Moment.feed))
    await eventually("the rows and the load time arrive") { !(list.rows.isEmpty || list.state?.loaded == nil) }

    #expect(list.rows.first?.uuid == "a")
    #expect(transport.calls.first?.operationName == "MomentsFeed")
    #expect(list.state?.hasMore == true && list.phase == .loaded)
    #expect(list.state?.total == 9)
    let entity = Projected<Moment>(try AccountScope().entity(Moment.self, "1"))
    await eventually("the entity shows the row") { entity.value != nil }
    #expect(entity.value?.uuid == "a")
  }

  @Test
  func theOutcomeOfASentCreateCarriesTheServerId() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)

    let sent = try await Moment.send(in: DB.account(), text: "hello", media: nil)
    #expect(sent.optimisticValue?.id.hasPrefix("temp") == true)
    #expect(try await sent.confirmed().id == "m77")
  }

  @Test(.timeLimit(.minutes(1)))
  func parallelOutcomeReadsOfOneReceiptAllGetTheServerId() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)

    let sent = try await Moment.send(in: DB.account(), text: "hello", media: nil)
    let ids = try await withThrowingTaskGroup(of: String.self) { group in
      for _ in 0..<8 { group.addTask { try await sent.confirmed().id } }
      return try await group.reduce(into: [String]()) { $0.append($1) }
    }

    #expect(ids == Array(repeating: "m77", count: 8))
  }

  @Test(.timeLimit(.minutes(1)))
  func aCreateSendsItsArgsAndTheRowNeverStoresThem() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt } }"}]"#)

    let confirmed = try await Record.send(in: DB.account(), containerId: "c", authorId: "u1", text: "hi", attachedUserId: "42", signedId: "blob-1").confirmed()
    let sent = transport.calls.first { $0.operationName == "CreateRecord" }?.variablesJson ?? ""
    #expect(sent.contains(#""signedId":"blob-1""#) && sent.contains(#""attachedUserId":42"#), "variables sent: \(sent)")
    #expect(try await Record.find(confirmed.id).current()?.text == "hi")
  }

  @Test(.timeLimit(.minutes(1)))
  func aCreateReadsTwoDeclaredLocalSources() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    transport.enqueue("CreateRecord", reply: #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt } }"},{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"CurrentRegionQuery","document":"query CurrentRegionQuery { region { id name } }"}]"#)
    transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"Ann"}}}"#)
    transport.enqueue("CurrentRegionQuery", reply: #"{"data":{"region":{"id":"r1","name":"Home"}}}"#)
    try await CurrentUser.current.load()
    try await CurrentRegion.current.load()
    let row = try await Record.send(in: DB.account(), containerId: "c", text: "hi").optimisticValue!
    #expect(row.authorId == "u1" && row.regionId == "r1")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aSnapshotCarriesTheTemporaryIdThatTheServerIdReplaced() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    let list = Projected<Moment>(try AccountScope().stream(Moment.recent))
    try await list.loaded()

    let local = try await Moment.send(in: DB.account(), text: "hello", media: nil).optimisticValue!
    await eventually("the list carries the replaced id") { list.projection.temporaryIds["m77"] != nil }

    #expect(list.projection.temporaryIds["m77"] == local.id)
  }


  @Test
  func aCreateWithAnUploadShowsTheRecordAtOnceAndTheServerIdReplacesIt() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateBlob", reply: #"{"data":{"createBlob":{"url":"https://s3/x","headers":{},"signedId":"sid","blobId":"b1"}}}"#)
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CreateBlob","document":"mutation CreateBlob($filename: String!, $contentType: String!, $byteSize: Int!, $checksum: String!) { createBlob(filename: $filename, contentType: $contentType, byteSize: $byteSize, checksum: $checksum) { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    let file = FileManager.default.temporaryDirectory.appending(path: "clip-\(UUID().uuidString).mp4")
    try Data("abcdef".utf8).write(to: file)

    let sent = try await DB.account().send(Moment.self, values: ["text": .string("hello")], upload: UploadCommand(action: "upload_clip", request: UploadRequest(path: file.path, variables: ["text": .string("hello")])))
    let local = try #require(sent.optimisticValue)

    #expect(local.id.hasPrefix("temp"))
    #expect(local.uploads["upload_clip"] != nil)
    #expect(try await sent.confirmed().id == "m77")
    let create = transport.calls.first { $0.operationName == "CreateMoment" }
    #expect(create?.variablesJson.contains("\"signedId\":\"sid\"") == true)
    #expect(create?.variablesJson.contains("blobId") == false)
    #expect(transport.calls.filter { $0.operationName == "CreateMoment" }.count == 1)
  }

  @MainActor
  @Test
  func aReactionUsesItsDeclaredKeyAcrossGeneratedValuesAndCells() async throws {
    _ = try await Self.openRecordSession()
    let record = try await Self.settledRecord()
    let row = try await record.update(reactions: [RecordReactionNode(body: "A", userId: "u1")]).optimisticValue!
    let changed = try await row.react(reactionBody: "C", userId: "u1").optimisticValue!
    #expect(changed.reactions.map(\.userId) == ["u1"])
    #expect(changed.reactions.map(\.body) == ["C"])
  }

  @MainActor
  @Test
  func anOptimisticReactionCarriesTheNestedUserOfTheLocalRow() async throws {
    _ = try await Self.openRecordSession()
    let ann = try await User.create(name: "Ann").optimisticValue!
    let record = try await Self.settledRecord()
    let withUser = try await record.react(reactionBody: "A", userId: ann.id).optimisticValue!
    #expect(withUser.reactions.first?.user?.name == "Ann")
    let withoutUser = try await record.react(reactionBody: "B", userId: "nobody").optimisticValue!
    #expect(withoutUser.reactions.first { $0.userId == "nobody" }?.user == nil)
  }

  private func openWithAMoment(_ transport: MockTransport) async throws -> Moment {
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"},{"name":"ShareMoment","document":"mutation ShareMoment($momentId: ID!) { shareMoment(momentId: $momentId) { moment { id uuid likedByMe media createdAt } successMessage copies } }"}]"#)
    let id = try await Moment.send(in: DB.account(), text: "hello", media: nil).confirmed().id
    return try #require(await Moment.find(id).current())
  }

  @Test
  func anActionAnswersWithTheDeclaredFieldsOfThePayloadAndRefusesWhenTheServerRefuses() async throws {
    let transport = MockTransport()
    let moment = try await openWithAMoment(transport)

    transport.enqueue("ShareMoment", reply: #"{"data":{"shareMoment":{"moment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"},"successMessage":"Shared","copies":3,"other":"x"}}}"#)
    let shared = try await moment.share()
    #expect(shared.optimisticValue?.id == "m77")
    let answer = try await shared.answer(as: MomentShareResult.self)
    #expect(answer.successMessage == "Shared")
    #expect(answer.copies == 3)
    #expect(try await shared.answer(as: MomentShareResult.self).copies == 3)

    transport.enqueue("ShareMoment", reply: #"{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}"#)
    let refused = try await moment.share()
    await eventually("the second ShareMoment call") { transport.calls.filter { $0.operationName == "ShareMoment" }.count >= 2 }
    #expect(try await refused.refusal().message == "not allowed")
  }

  @Test
  func anActionAnswersWhenEveryFieldOfThePayloadIsNull() async throws {
    let transport = MockTransport()
    let moment = try await openWithAMoment(transport)

    transport.enqueue("ShareMoment", reply: #"{"data":{"shareMoment":{"moment":null,"successMessage":null,"copies":null}}}"#)
    let answer = try await moment.share().answer(as: MomentShareResult.self)
    #expect(answer.successMessage == nil)
    #expect(answer.copies == nil)
    #expect(try await Moment.find("m77").current() != nil)
  }
}

extension BridgeProbeTests {

  /// REL-24: a reset of the account empties the entities that readers hold and the same objects receive the next rows.

  /// REL-25: closing the database closes the held entities and the next open starts empty.
}
