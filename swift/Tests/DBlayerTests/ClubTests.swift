import DBlayerTesting
import Foundation
import SQLite3
import Testing
@testable import DBlayer

private let clubCreated = #"{"data":{"createClub":{"__typename":"Club","id":"g1","name":"friends"}}}"#
private let letterSent = #"{"data":{"sendLetter":{"__typename":"Letter","id":"l1","chatId":"c1","text":"hello","at":1}}}"#
private let clubDefs = #"[{"name":"CreateClub","document":"mutation CreateClub($name: String!) { createClub(name: $name) { id name } }"},{"name":"LeaveClub","document":"mutation LeaveClub($clubId: ID!) { leaveClub(clubId: $clubId) { id name } }"},{"name":"AddClubMember","document":"mutation AddClubMember($clubId: ID!, $userIds: [ID!]!) { addClubMember(clubId: $clubId, userIds: $userIds) { id name } }"},{"name":"RemoveClubMember","document":"mutation RemoveClubMember($clubId: ID!, $userIds: [ID!]!) { removeClubMember(clubId: $clubId, userIds: $userIds) { id name } }"},{"name":"ClubMembers","document":"query ClubMembers($clubId: ID!, $order: String!, $first: Int, $after: Cursor) { node(id: $clubId) { ... on Club { members(first: $first, after: $after, order: $order) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"},{"name":"SendLetter","document":"mutation SendLetter($chatId: ID!, $text: String!, $at: Int!) { sendLetter(chatId: $chatId, text: $text, at: $at) { id chatId text at } }"},{"name":"ForwardLetter","document":"mutation ForwardLetter($letterId: ID!, $chatId: ID!) { forwardLetter(letterId: $letterId, chatId: $chatId) { letter { id chatId text at } } }"}]"#

func clubAnswer(_ root: String) -> String {
  #"{"data":{"\#(root)":{"__typename":"Club","id":"g1","name":"friends"}}}"#
}
func membersReply(_ ids: [String]) -> String {
  let edges = ids.map { #"{"cursor":"\#($0)","node":{"id":"\#($0)","name":"\#($0)"}}"# }.joined(separator: ",")
  return #"{"data":{"node":{"members":{"edges":[\#(edges)],"pageInfo":{"hasNextPage":false,"endCursor":"\#(ids.last ?? "")"}}}}}"#
}

private let forbidden = DbError.Http(status: 403, detail: "forbidden")


private func forwardAnswer(_ id: String, chat: String) -> String {
  #"{"data":{"forwardLetter":{"letter":{"__typename":"Letter","id":"\#(id)","chatId":"\#(chat)","text":"hello","at":99}}}}"#
}

private final class MemberEventsTransport: Transport, @unchecked Sendable {
  let base = MockTransport()
  let opened: AsyncStream<Void>
  private let signal: AsyncStream<Void>.Continuation
  init() {
    let (stream, continuation) = AsyncStream<Void>.makeStream()
    opened = stream
    signal = continuation
  }
  func openCable(lease: CableLease) async throws { try await base.openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await base.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await base.cableNext(token: token) }
  func cableClose(token: String) async { await base.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await base.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws {
    try await base.subscribe(request: request, sink: sink)
    if request.operationName == "MemberChanged" { signal.yield(()) }
  }
  func unsubscribe(token: String) { base.unsubscribe(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, tag: String, progress: ByteProgress) async throws -> PutReply {
    try await base.put(url: url, headers: headers, file: file, tag: tag, progress: progress)
  }
}

private func clubModels(keep: Bool = false, complete: Bool = false, keepLetter: Bool = false, events: Bool = false, destroy: Bool = false) throws -> String {
  guard keep || complete || keepLetter || events || destroy else { return modelsJSON }
  var models = try JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as! [[String: Any]]
  if keep {
    let index = models.firstIndex { $0["key"] as? String == "Club" }!
    var actions = models[index]["actions"] as! [[String: Any]]
    let create = actions.firstIndex { $0["name"] as? String == "create" }!
    actions[create]["on_failure"] = "keep"
    models[index]["actions"] = actions
  }
  if complete || events {
    let index = models.firstIndex { $0["key"] as? String == "Person" }!
    var relations = models[index]["relations"] as! [[String: Any]]
    let member = relations.firstIndex { $0["name"] as? String == "club_members" }!
    if complete {
      var remote = relations[member]["remote"] as! [String: Any]
      var shape = remote["shape"] as! [String: Any]
      shape["coverage"] = "complete"
      remote["shape"] = shape
      relations[member]["remote"] = remote
    }
    if events { relations[member]["updates"] = [["subscription": "MemberChanged", "set": [:], "removes": false]] }
    models[index]["relations"] = relations
  }
  if keepLetter {
    let index = models.firstIndex { $0["key"] as? String == "Letter" }!
    var actions = models[index]["actions"] as! [[String: Any]]
    let create = actions.firstIndex { $0["name"] as? String == "create" }!
    actions[create]["on_failure"] = "keep"
    models[index]["actions"] = actions
  }
  if destroy {
    let index = models.firstIndex { $0["key"] as? String == "Club" }!
    var actions = models[index]["actions"] as! [[String: Any]]
    actions.append(["name": "destroy", "root_shape": "destroy", "mutation": "DeleteClub", "variables": ["id": "id"]])
    models[index]["actions"] = actions
  }
  return String(decoding: try JSONSerialization.data(withJSONObject: models), as: UTF8.self)
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anOperationOutcomeCanBeReadTwiceFromItsReceipt() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated)
    transport.enqueue("AddClubMember", reply: clubAnswer("addClubMember"))
    transport.enqueue("LeaveClub", reply: clubAnswer("leaveClub"))
    try await Self.openClubSession(transport)
    let club = try await Self.confirmedClub()
    let receipt = try await club.addMember(userIds: ["u9"])
    let first = Task { try await receipt.outcome() }
    guard case let .confirmed(value, _) = try await valueBeforeDeadline(first, release: {}), value?.id == club.id else { throw WaitFailure.unexpectedOutcome }
    let leaving = try await club.leave()
    let left = Task { try await leaving.outcome() }
    guard case .confirmed = try await valueBeforeDeadline(left, release: {}) else { throw WaitFailure.unexpectedOutcome }
    let second = Task { try await receipt.outcome() }
    guard case let .confirmed(value, _) = try await valueBeforeDeadline(second, release: {}), value?.id == club.id else { throw WaitFailure.unexpectedOutcome }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func aPendingOperationSurvivesAnOldFormatWithoutRestoringTheCachedRow(keep: Bool) async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let path = directory.appendingPathComponent("old.sqlite").path
    let config = Config(path: path, dataVersion: nil, cacheRows: 32)
    let models = try clubModels(keepLetter: keep)
    try await reopenSigned(config: config, modelsJson: models, operations: clubDefs)
    let receipt = try await Letter.send(in: DB.account(), chatId: "c1", text: "held", at: 1)
    let id = try #require(receipt.optimisticValue?.id)
    try await DB.close()
    var connection: OpaquePointer?
    try #require(sqlite3_open(path, &connection) == SQLITE_OK)
    let changed = sqlite3_exec(connection, "DROP TABLE IF EXISTS op_effect_value; DROP TABLE IF EXISTS op_effect; DROP TABLE IF EXISTS scope_revision; UPDATE manifest SET value = 'old' WHERE key = 'format_version'", nil, nil, nil)
    sqlite3_close(connection)
    try #require(changed == SQLITE_OK)
    let transport = MockTransport()
    transport.enqueue("SendLetter", failure: .Http(status: 422, detail: "refused"))
    let database = try await reopen(config: config, modelsJson: models, transport: transport, operations: clubDefs)
    let flushing = Task { try await database.flush() }
    try await valueBeforeDeadline(flushing) {}
    #expect(transport.calls.filter { $0.operationName == "SendLetter" }.count == 1)
    #expect(try await Letter.ids([id]).read().isEmpty)
    transport.enqueue("SendLetter", failure: .Http(status: 422, detail: "refused again"))
    let retry = Task { try await Letter.retry(id: id, action: nil) }
    do {
      let retried = try await valueBeforeDeadline(retry) {}
      #expect(keep)
      let waiting = Task { try await retried.outcome() }
      guard case .rejected = try await valueBeforeDeadline(waiting, release: {}) else { throw WaitFailure.unexpectedOutcome }
      #expect(try await Letter.ids([id]).read().isEmpty)
    } catch DbError.Database {
      #expect(!keep)
    }
    try await DB.close()
  }

  @MainActor
  static func confirmedClub() async throws -> Club {
    let creating = Task { try await Club.create(name: "friends") }
    let receipt = try await valueBeforeDeadline(creating) {}
    let waiting = Task { try await receipt.outcome() }
    guard case .confirmed = try await valueBeforeDeadline(waiting, release: {}) else { throw WaitFailure.unexpectedOutcome }
    return try #require(try await Query<Club>().read().first)
  }

  @MainActor
  static func removeMemberConfirmed(_ club: Club) async throws {
    let removing = Task { try await club.removeMember(userIds: ["u4"]) }
    let receipt = try await valueBeforeDeadline(removing) {}
    let waiting = Task { try await receipt.outcome() }
    guard case .confirmed = try await valueBeforeDeadline(waiting, release: {}) else { throw WaitFailure.unexpectedOutcome }
  }

  @MainActor
  static func confirmedLetter() async throws -> Letter {
    let sending = Task { try await Letter.send(in: DB.account(), chatId: "c1", text: "hello", at: 1) }
    let receipt = try await valueBeforeDeadline(sending) {}
    let waiting = Task { try await receipt.outcome() }
    guard case .confirmed = try await valueBeforeDeadline(waiting, release: {}) else { throw WaitFailure.unexpectedOutcome }
    return try #require(try await Query<Letter>().read().first)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func aRefusedDestroyRestoresTheRowAccordingToItsDeclaration(business: Bool) async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated)
    try await Self.openClubSession(transport, models: clubModels(destroy: true))
    try DB.database.registerOperations(defsJson: #"[{"name":"DeleteClub","document":"mutation DeleteClub($id: ID!) { deleteClub(id: $id) { id } }"}]"#)
    let club = try await Self.confirmedClub()
    let hold = business
      ? transport.hold("DeleteClub", reply: #"{"errors":[{"message":"refused","extensions":{"code":"REFUSED"}}]}"#)
      : transport.hold("DeleteClub", failure: .Http(status: 422, detail: "refused"))
    defer { hold.release() }
    let receipt = try await club.destroy()
    try #require(await entered(hold), "destroy did not reach the transport")
    #expect(try await Club.ids([club.id]).read().isEmpty)
    hold.release()
    let waiting = Task { try await receipt.outcome() }
    let result = try await valueBeforeDeadline(waiting) { hold.release() }
    if case .rejected = result {} else { Issue.record("destroy was not rejected") }
    #expect(try await Club.ids([club.id]).read().count == 1)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func joiningMultipleMembersKeepsEachOnceInOrder() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated)
    transport.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3", "u4", "u5"]))
    try await Self.openClubSession(transport)
    let club = try await Self.confirmedClub()
    let joined = try Person.clubMembers(club.id, "JOINED")
    try await joined.load()
    transport.enqueue("RemoveClubMember", reply: clubAnswer("removeClubMember"))
    let removed = try await club.removeMember(userIds: ["u4", "u5"])
    let removedOutcome = Task { try await removed.outcome() }
    guard case .confirmed = try await valueBeforeDeadline(removedOutcome, release: {}) else { throw WaitFailure.unexpectedOutcome }
    let hold = transport.hold("AddClubMember", reply: clubAnswer("addClubMember"))
    defer { hold.release() }
    let adding = Task { try await club.addMember(userIds: ["u4", "u5", "u4"]) }
    try #require(await entered(hold), "join did not reach the transport")
    let receipt = try await valueBeforeDeadline(adding) { hold.release() }
    #expect(try await joined.read().map(\.id) == ["u1", "u2", "u3", "u4", "u5"])
    hold.release()
    let outcome = Task { try await receipt.outcome() }
    guard case .confirmed = try await valueBeforeDeadline(outcome, release: {}) else { throw WaitFailure.unexpectedOutcome }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aMemberEventKeepsTheOpenJoinInTheList() async throws {
    let transport = MemberEventsTransport()
    transport.base.enqueue("CreateClub", reply: clubCreated)
    transport.base.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3", "u4"]))
    try await Self.openClubSession(transport, models: clubModels(events: true))
    try DB.database.registerOperations(defsJson: #"[{"name":"MemberChanged","document":"subscription MemberChanged { memberChanged { id name } }"}]"#)
    let club = try await Self.confirmedClub()
    let joined = try Person.clubMembers(club.id, "JOINED")
    try await joined.load()
    transport.base.enqueue("RemoveClubMember", reply: clubAnswer("removeClubMember"))
    try await Self.removeMemberConfirmed(club)
    try #require(try await joined.read().allSatisfy { $0.id != "u4" })
    let stream = try DB.account().stream(joined)
    defer { stream.cancel() }
    let cursor = Cursor(stream)
    try await stream.loaded()
    let opened = Cursor(transport.opened)
    try #require(await opened.next("member subscription", timeout: .seconds(10)) != nil)
    let hold = transport.base.hold("AddClubMember", reply: clubAnswer("addClubMember"))
    defer { hold.release() }
    let adding = Task { try await club.addMember(userIds: ["u4"]) }
    try #require(await entered(hold), "join did not reach the transport")
    try transport.base.emit("MemberChanged", json: #"{"data":{"memberChanged":{"id":"u1","name":"updated"}}}"#)
    var projection = ListProjection<Person>()
    await eventually("the member event", timeout: .seconds(10)) {
      if projection.rows.contains(where: { $0.id == "u1" && $0.name == "updated" }) { return true }
      if let delivery = await cursor.next("the member event", timeout: .seconds(10)) { _ = projection.apply(delivery) }
      return projection.rows.contains(where: { $0.id == "u1" && $0.name == "updated" })
    }
    try #require(projection.rows.contains { $0.id == "u1" && $0.name == "updated" })
    #expect(projection.rows.map(\.id).contains("u4"))
    let receipt = try await valueBeforeDeadline(adding) { hold.release() }
    transport.base.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3", "u4"]))
    hold.release()
    let outcome = Task { try await receipt.outcome() }
    let result = try await valueBeforeDeadline(outcome) { hold.release() }
    if case .confirmed = result {} else { Issue.record("the join was not confirmed") }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func aRefusedDurableSendKeepsItsRowWhenDeclared(retry: Bool) async throws {
    let transport = MockTransport()
    try await Self.openClubSession(transport, models: clubModels(keepLetter: true))
    let hold = transport.hold("SendLetter", failure: .Http(status: 422, detail: "refused"))
    if retry { transport.enqueue("SendLetter", reply: letterSent) }
    defer { hold.release() }
    let sent = try await Letter.send(in: DB.account(), chatId: "c1", text: "hello", at: 1)
    let local = try #require(sent.optimisticValue)
    try #require(await entered(hold), "send did not reach the transport")
    #expect(try await Letter.ids([local.id]).read().count == 1)
    hold.release()
    let outcome = Task { try await sent.outcome() }
    let result = try await valueBeforeDeadline(outcome) { hold.release() }
    if case .rejected = result {} else { Issue.record("the refused send was not rejected") }
    #expect(try await Letter.ids([local.id]).read().count == 1)
    if retry {
      let retried = try await local.retry(action: nil)
      let result = try await retried.outcome()
      if case .confirmed = result {} else { Issue.record("retry was not confirmed") }
    } else {
      try await local.discard(action: nil)
    }
    #expect(try await Query<Letter>().read().count == (retry ? 1 : 0))
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func aLaterConfirmedMembershipChangeSurvivesAnEarlierRefusal(firstJoin: Bool) async throws {
    let transport = MockTransport()
    let (club, joined, _) = try await Self.openClubWithMembers(transport)
    if firstJoin {
      transport.enqueue("RemoveClubMember", reply: clubAnswer("removeClubMember"))
      try await Self.removeMemberConfirmed(club)
    }
    try #require(try await joined.read().map(\.id).contains("u4") == !firstJoin)
    let old = transport.hold("ClubMembers", reply: membersReply(firstJoin ? ["u1", "u2", "u3"] : ["u1", "u2", "u3", "u4"]))
    defer { old.release() }
    let reading = Task { try await joined.refresh() }
    try #require(await entered(old), "the old list did not reach the transport")
    let first = transport.hold(firstJoin ? "AddClubMember" : "RemoveClubMember", failure: forbidden)
    let second = transport.hold(firstJoin ? "RemoveClubMember" : "AddClubMember", reply: clubAnswer(firstJoin ? "removeClubMember" : "addClubMember"))
    defer { first.release(); second.release() }
    let joining = Task { firstJoin ? try await club.addMember(userIds: ["u4"]) : try await club.removeMember(userIds: ["u4"]) }
    try #require(await entered(first), "join did not reach the transport")
    let firstReceipt = try await valueBeforeDeadline(joining) { first.release(); second.release() }
    first.release()
    let earlier = Task { try await firstReceipt.outcome() }
    let earlierOutcome = try await valueBeforeDeadline(earlier) { first.release(); second.release() }
    if case .rejected = earlierOutcome {} else { Issue.record("the earlier change was not rejected") }
    let leaving = Task { firstJoin ? try await club.removeMember(userIds: ["u4"]) : try await club.addMember(userIds: ["u4"]) }
    let secondReceipt = try await valueBeforeDeadline(leaving) { first.release(); second.release() }
    try #require(await entered(second), "the later change did not reach the transport after the earlier one ended")
    second.release()
    let later = Task { try await secondReceipt.outcome() }
    let laterOutcome = try await valueBeforeDeadline(later) { first.release(); second.release() }
    if case .confirmed = laterOutcome {} else { Issue.record("the later change was not confirmed") }
    old.release()
    try await valueBeforeDeadline(reading) { old.release(); first.release(); second.release() }
    #expect(try await joined.read().map(\.id).contains("u4") == !firstJoin)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func forwardReturnsItsOptimisticValueBeforeTheServerAnswers() async throws {
    let transport = MockTransport()
    transport.enqueue("SendLetter", reply: letterSent)
    try await Self.openClubSession(transport)
    let source = try await Self.confirmedLetter()
    let sourceId = source.id
    let hold = transport.hold("ForwardLetter", reply: forwardAnswer("fwd-1", chat: "c2"))
    defer { hold.release() }
    let forward = Task { try await source.forward(targetChatId: "c2") }
    try #require(await entered(hold), "forward did not reach the transport")
    let receipt = try await valueBeforeDeadline(forward) { hold.release() }
    let optimistic = try #require(receipt.optimisticValue)
    #expect(optimistic.id != "fwd-1" && optimistic.chatId == "c2")
    let written = try #require(try await DB.database.readRow(epoch: DB.database.sessionEpoch(), model: "Letter", rowId: optimistic.id))
    #expect(receipt.revision == written.revision)
    let unchanged = try #require(try await Letter.ids([sourceId]).read().first)
    #expect(unchanged.chatId == source.chatId && unchanged.text == source.text)
    hold.release()
    let waiting = Task { try await receipt.outcome() }
    let outcome = try await valueBeforeDeadline(waiting) { hold.release() }
    #expect(outcome.confirmedValue?.id == "fwd-1")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anOfflineActionIsNotSentAfterReconnectButALetterIsSentOnce() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated)
    transport.enqueue("SendLetter", reply: letterSent)
    try await Self.openClubSession(transport)
    let club = try await Self.confirmedClub()
    try DB.database.setOnline(online: false)
    do {
      _ = try await club.addMember(userIds: ["u9"])
      Issue.record("the offline action succeeded")
    } catch {}
    let sent = try await Letter.send(in: DB.account(), chatId: "c1", text: "hello", at: 1)
    #expect(sent.optimisticValue != nil)
    #expect(transport.calls.filter { $0.operationName == "SendLetter" }.isEmpty)
    try DB.database.setOnline(online: true)
    let outcome = try await sent.outcome()
    if case .confirmed = outcome {} else { Issue.record("send was not confirmed") }
    #expect(transport.calls.filter { $0.operationName == "SendLetter" }.count == 1)
    #expect(transport.calls.filter { $0.operationName == "AddClubMember" }.isEmpty)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: [(false, false), (false, true), (true, false), (true, true)])
  func aCreateRefusalFollowsTheDeclaredPolicy(business: Bool, keep: Bool) async throws {
    let transport = MockTransport()
    if business { transport.enqueue("CreateClub", reply: #"{"errors":[{"message":"refused","extensions":{"code":"REFUSED"}}]}"#) } else { transport.enqueue("CreateClub", failure: .Http(status: 422, detail: "refused")) }
    try await Self.openClubSession(transport, models: clubModels(keep: keep))
    let receipt = try await Club.create(name: "friends")
    let outcome = try await receipt.outcome()
    if case .rejected = outcome {} else { Issue.record("create was not rejected") }
    #expect(transport.calls.filter { $0.operationName == "CreateClub" }.count == 1)
    #expect(try await Query<Club>().read().count == (keep ? 1 : 0))
  }

  @MainActor
  @Test(.timeLimit(.minutes(2)), arguments: [(false, false, false), (false, true, false), (true, false, false), (true, true, false), (false, false, true), (false, true, true), (true, false, true), (true, true, true)])
  func anAcknowledgedMembershipChangeSurvivesAnOlderListAnswer(removing: Bool, answerFirst: Bool, complete: Bool) async throws {
    let transport = MockTransport()
    let (club, joined, _) = try await Self.openClubWithMembers(transport, models: clubModels(complete: complete))
    if !removing {
      transport.enqueue("RemoveClubMember", reply: clubAnswer("removeClubMember"))
      try await Self.removeMemberConfirmed(club)
    }
    let old = removing ? ["u1", "u2", "u3", "u4"] : ["u1", "u2", "u3"]
    try #require(try await joined.read().map(\.id).contains("u4") == removing)
    let hold = transport.hold("ClubMembers", reply: membersReply(old))
    defer { hold.release() }
    let reading = Task { try await joined.refresh() }
    try #require(await entered(hold), "the list request did not reach the transport")
    let mutation = transport.hold(removing ? "RemoveClubMember" : "AddClubMember", reply: clubAnswer(removing ? "removeClubMember" : "addClubMember"))
    defer { mutation.release() }
    let acting = Task { removing ? try await club.removeMember(userIds: ["u4"]) : try await club.addMember(userIds: ["u4"]) }
    try #require(await entered(mutation), "the mutation did not reach the transport")
    let receipt = try await valueBeforeDeadline(acting) { hold.release(); mutation.release() }
    if answerFirst {
      mutation.release()
      let waiting = Task { try await receipt.outcome() }
      let result = try await valueBeforeDeadline(waiting) { hold.release(); mutation.release() }
      if case .confirmed = result {} else { Issue.record("the mutation was not confirmed") }
    }
    hold.release()
    try await valueBeforeDeadline(reading) { hold.release(); mutation.release() }
    #expect(try await joined.read().map(\.id).contains("u4") == !removing)
    if !answerFirst {
      mutation.release()
      let waiting = Task { try await receipt.outcome() }
      let result = try await valueBeforeDeadline(waiting) { hold.release(); mutation.release() }
      if case .confirmed = result {} else { Issue.record("the mutation was not confirmed") }
    }
  }

  @MainActor
  static func openClubSession(_ transport: any Transport, models: String = modelsJSON) async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: models, transport: transport)
    try DB.database.registerOperations(defsJson: clubDefs)
  }

  /// K04 (B4): a create the server answers returns the confirmed row, needs the network, and queues nothing offline.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aServerCreateReturnsTheConfirmedRowAndNeedsTheNetwork() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated, delay: 0.3)
    try await Self.openClubSession(transport)
    let club = try await confirmedClub()
    #expect(club.id == "g1" && club.name == "friends" && club.sync.state == .synced, "the call returned before the server answered")
    let creates = { transport.calls.filter { $0.operationName == "CreateClub" }.count }
    #expect(creates() == 1)

    try DB.database.setOnline(online: false)
    do {
      _ = try await Club.create(name: "offline")
      Issue.record("a server create offline did not fail")
    } catch DbError.Network {
    }
    #expect(creates() == 1)
  }

  /// Opens a session with a club whose two member lists hold u1 u2 u3 u4 (JOINED) and u2 u1 u3 u4 (OWNER_FIRST).
  @MainActor
  static func openClubWithMembers(_ transport: MockTransport, models: String = modelsJSON) async throws -> (Club, Query<Person>, Query<Person>) {
    transport.enqueue("CreateClub", reply: clubCreated)
    transport.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3", "u4"]))
    transport.enqueue("ClubMembers", reply: membersReply(["u2", "u1", "u3", "u4"]))
    try await openClubSession(transport, models: models)
    let club = try await confirmedClub()
    let joined = try Person.clubMembers(club.id, "JOINED")
    let owners = try Person.clubMembers(club.id, "OWNER_FIRST")
    try await joined.load()
    try await owners.load()
    return (club, joined, owners)
  }

  /// K13 (B6, C05): remove member and add member change both lists before the answer. The answer keeps the change, a refusal restores the ids and the order of both lists.
  @MainActor
  @Test(.timeLimit(.minutes(2)), arguments: [(true, false), (true, true), (false, false), (false, true)])
  func aMemberActionChangesBothListsBeforeTheAnswerAndARefusalRestoresThem(removing: Bool, refusal: Bool) async throws {
    let case_ = "\(removing ? "remove" : "add") \(refusal ? "refused" : "answered")"
    let transport = MockTransport()
    let (club, joined, owners) = try await Self.openClubWithMembers(transport)
    func lists() async throws -> [[String]] { [try await joined.read().map(\.id), try await owners.read().map(\.id)] }
    let original = [["u1", "u2", "u3", "u4"], ["u2", "u1", "u3", "u4"]]
    if !removing {
      transport.enqueue("RemoveClubMember", reply: clubAnswer("removeClubMember"))
      try await Self.removeMemberConfirmed(club)
      #expect(try await lists() == [["u1", "u2", "u3"], ["u2", "u1", "u3"]], "\(case_): the setup")
    }
    let user = removing ? "u2" : "u4"
    let operation = removing ? "RemoveClubMember" : "AddClubMember"
    let answer = clubAnswer(removing ? "removeClubMember" : "addClubMember")
    let hold = transport.hold(operation, reply: answer, failure: refusal ? forbidden : nil)
    defer { hold.release() }
    let acting = Task { removing ? try await club.removeMember(userIds: [user]) : try await club.addMember(userIds: [user]) }
    try #require(await entered(hold), "the action did not reach the transport")
    let during = removing ? [["u1", "u3", "u4"], ["u1", "u3", "u4"]] : original
    #expect(try await lists() == during, "\(case_): the lists before the answer")
    let receipt = try await valueBeforeDeadline(acting) { hold.release() }
    hold.release()
    let waiting = Task { try await receipt.outcome() }
    let outcome = try await valueBeforeDeadline(waiting) { hold.release() }
    if refusal {
      if case .rejected = outcome {} else { Issue.record("\(case_): the outcome") }
    } else {
      if case .confirmed = outcome {} else { Issue.record("\(case_): the outcome") }
    }
    let after = refusal ? (removing ? original : [["u1", "u2", "u3"], ["u2", "u1", "u3"]]) : during
    #expect(try await lists() == after, "\(case_): the lists after the outcome")
  }

  /// A member whose row is not cached is skipped by the optimistic join: the call ends with the answer and the lists keep their ids.
  @MainActor
  @Test(.timeLimit(.minutes(2)))
  func addingAMemberWhoseRowIsNotCachedEndsWithTheAnswer() async throws {
    let transport = MockTransport()
    let (club, joined, owners) = try await Self.openClubWithMembers(transport)
    transport.enqueue("AddClubMember", reply: clubAnswer("addClubMember"))
    let add = try await club.addMember(userIds: ["u9"])
    guard case .confirmed = try await add.outcome() else { throw WaitFailure.unexpectedOutcome }
    #expect(transport.calls.filter { $0.operationName == "AddClubMember" }.count == 1)
    #expect(try await joined.read().map(\.id) == ["u1", "u2", "u3", "u4"])
    #expect(try await owners.read().map(\.id) == ["u2", "u1", "u3", "u4"])
  }

  /// K32: a member added or removed ends with the member lists of the server in both orders. The added user appears in both lists and the removed one leaves them.
  @MainActor
  @Test(.timeLimit(.minutes(2)))
  func anAddedAndARemovedMemberEndWithTheMemberListsOfTheServer() async throws {
    let transport = MockTransport()
    let (club, joined, owners) = try await Self.openClubWithMembers(transport)
    func lists() async throws -> [[String]] { [try await joined.read().map(\.id), try await owners.read().map(\.id)] }
    func readAgain(_ joinedIds: [String], _ ownerIds: [String]) async throws {
      transport.enqueue("ClubMembers", reply: membersReply(joinedIds))
      try await joined.load()
      transport.enqueue("ClubMembers", reply: membersReply(ownerIds))
      try await owners.load()
    }
    let added = [["u1", "u2", "u3", "u4", "u9"], ["u2", "u1", "u3", "u4", "u9"]]
    transport.enqueue("AddClubMember", reply: clubAnswer("addClubMember"))
    let add = try await club.addMember(userIds: ["u9"])
    guard case .confirmed = try await add.outcome() else { throw WaitFailure.unexpectedOutcome }
    try await readAgain(added[0], added[1])
    #expect(try await lists() == added, "the lists after the add")
    let removed = [["u1", "u2", "u3", "u4"], ["u2", "u1", "u3", "u4"]]
    transport.enqueue("RemoveClubMember", reply: clubAnswer("removeClubMember"))
    let remove = try await club.removeMember(userIds: ["u9"])
    guard case .confirmed = try await remove.outcome() else { throw WaitFailure.unexpectedOutcome }
    try await readAgain(removed[0], removed[1])
    #expect(try await lists() == removed, "the lists after the removal")
  }

  /// K13 (B6, C05): leave removes the club from its list before the answer. The answer keeps it away, a refusal puts it back.
  @MainActor
  @Test(.timeLimit(.minutes(2)), arguments: [false, true])
  func leaveRemovesTheClubFromItsListBeforeTheAnswerAndARefusalRestoresIt(refusal: Bool) async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated)
    try await Self.openClubSession(transport)
    let club = try await Self.confirmedClub()
    let hold = transport.hold("LeaveClub", reply: clubAnswer("leaveClub"), failure: refusal ? forbidden : nil)
    let leaving = Task { try await club.leave() }
    defer { hold.release() }
    try #require(await entered(hold))
    #expect(try await Club.ids([club.id]).read().isEmpty, "the club stayed in its list before the answer")
    hold.release()
    let receipt = try await valueBeforeDeadline(leaving) { hold.release() }
    let waiting = Task { try await receipt.outcome() }
    let outcome = try await valueBeforeDeadline(waiting) { hold.release() }
    if case .rejected = outcome { #expect(refusal) } else { #expect(!refusal) }
    await eventually("the club list after the outcome") { ((try? await Club.ids([club.id]).read().map(\.id)) ?? ["?"]) == (refusal ? [club.id] : []) }
  }

  /// K12: a forward into the same chat and into another chat leaves the source row unchanged, upserts the answer row in its own chat, and the call returns the answer row.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aForwardLeavesTheSourceUnchangedUpsertsTheAnswerRowAndReturnsIt() async throws {
    let transport = MockTransport()
    transport.enqueue("SendLetter", reply: letterSent)
    try await Self.openClubSession(transport)
    let source = try await Self.confirmedLetter()
    let sourceId = source.id

    for (target, id) in [("c1", "fwd-1"), ("c2", "fwd-2")] {
      transport.enqueue("ForwardLetter", reply: forwardAnswer(id, chat: target))
      let receipt = try await source.forward(targetChatId: target)
      let answer = try #require(try await receipt.outcome().confirmedValue)
      #expect(answer.id == id && answer.chatId == target, "\(target): the call returned \(answer.id), not the answer row")
      let unchanged = try #require(try await Letter.ids([sourceId]).read().first)
      #expect(unchanged.chatId == "c1" && unchanged.text == "hello" && unchanged.at == 1, "\(target): the answer was written onto the source row")
      let upserted = try #require(try await Letter.ids([id]).read().first)
      #expect(upserted.chatId == target, "\(target): the answer row is not in its own chat")
    }
    let sameChat = try await Letter.thread("c1").read()
    #expect(sameChat.map(\.id).contains(sourceId) && sameChat.map(\.id).contains("fwd-1"))
  }

  /// K22 (B6, C05): a forward shows a local copy in the thread of the target chat before the answer. The answer replaces the copy, a refusal removes it.
  @MainActor
  @Test(.timeLimit(.minutes(2)), arguments: [false, true])
  func aForwardShowsALocalCopyInTheTargetThreadAndARefusalRemovesIt(refusal: Bool) async throws {
    let transport = MockTransport()
    transport.enqueue("SendLetter", reply: letterSent)
    try await Self.openClubSession(transport)
    let source = try await Self.confirmedLetter()
    let thread = try Letter.thread("c2")
    #expect(try await thread.read().isEmpty)

    let hold = transport.hold("ForwardLetter", reply: forwardAnswer("fwd-1", chat: "c2"), failure: refusal ? forbidden : nil)
    let forwarding = Task { try await source.forward(targetChatId: "c2") }
    defer { hold.release() }
    try #require(await entered(hold))
    let copy = try #require(try await thread.read().first, "the target thread has no copy before the answer")
    #expect(copy.id != "fwd-1" && copy.text == "hello" && copy.chatId == "c2", "the copy is \(copy.id)")
    hold.release()
    let receipt = try await valueBeforeDeadline(forwarding) { hold.release() }
    let waiting = Task { try await receipt.outcome() }
    let result = try await valueBeforeDeadline(waiting) { hold.release() }
    if case .rejected = result { #expect(refusal) } else { #expect(!refusal) }
    await eventually("the target thread shows the outcome") { ((try? await thread.read().map(\.id)) ?? ["?"]) == (refusal ? [] : ["fwd-1"]) }
  }
}
