import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let clubCreated = #"{"data":{"createClub":{"__typename":"Club","id":"g1","name":"friends"}}}"#
private let letterSent = #"{"data":{"sendLetter":{"__typename":"Letter","id":"l1","chatId":"c1","text":"hello","at":1}}}"#
private let clubDefs = #"[{"name":"CreateClub","document":"mutation CreateClub($name: String!) { createClub(name: $name) { id name } }"},{"name":"LeaveClub","document":"mutation LeaveClub($clubId: ID!) { leaveClub(clubId: $clubId) { id name } }"},{"name":"AddClubMember","document":"mutation AddClubMember($clubId: ID!, $userIds: [ID!]!) { addClubMember(clubId: $clubId, userIds: $userIds) { id name } }"},{"name":"RemoveClubMember","document":"mutation RemoveClubMember($clubId: ID!, $userIds: [ID!]!) { removeClubMember(clubId: $clubId, userIds: $userIds) { id name } }"},{"name":"ClubMembers","document":"query ClubMembers($clubId: ID!, $order: String!, $first: Int, $after: Cursor) { node(id: $clubId) { ... on Club { members(first: $first, after: $after, order: $order) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"},{"name":"SendLetter","document":"mutation SendLetter($chatId: ID!, $text: String!, $at: Int!) { sendLetter(chatId: $chatId, text: $text, at: $at) { id chatId text at } }"},{"name":"ForwardLetter","document":"mutation ForwardLetter($letterId: ID!, $chatId: ID!) { forwardLetter(letterId: $letterId, chatId: $chatId) { letter { id chatId text at } } }"}]"#

private func clubAnswer(_ root: String) -> String {
  #"{"data":{"\#(root)":{"__typename":"Club","id":"g1","name":"friends"}}}"#
}

private func membersReply(_ ids: [String]) -> String {
  let edges = ids.map { #"{"cursor":"\#($0)","node":{"id":"\#($0)","name":"\#($0)"}}"# }.joined(separator: ",")
  return #"{"data":{"node":{"members":{"edges":[\#(edges)],"pageInfo":{"hasNextPage":false,"endCursor":"\#(ids.last ?? "")"}}}}}"#
}

private let forbidden = DbError.Http(status: 403, detail: "forbidden")

private extension MockTransport {
  func enqueueOutcome(_ operation: String, reply: String, refusal: Bool, delay: TimeInterval) {
    if refusal { enqueue(operation, failure: forbidden, delay: delay) } else { enqueue(operation, reply: reply, delay: delay) }
  }
}

private func forwardAnswer(_ id: String, chat: String) -> String {
  #"{"data":{"forwardLetter":{"letter":{"__typename":"Letter","id":"\#(id)","chatId":"\#(chat)","text":"hello","at":99}}}}"#
}

extension BridgeProbeTests {
  @MainActor
  static func openClubSession(_ transport: MockTransport) async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: clubDefs)
  }

  /// K04 (B4): a create the server answers returns the confirmed row, needs the network, and queues nothing offline.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aServerCreateReturnsTheConfirmedRowAndNeedsTheNetwork() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated, delay: 0.3)
    try await Self.openClubSession(transport)
    let club = try await Club.create(name: "friends")
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

  /// K13 (B6, C05): leave and the member actions change the real lists before the answer. The answer keeps the change, a refusal restores the ids and the order of the lists.
  @MainActor
  @Test(.timeLimit(.minutes(2)))
  func aClubActionChangesItsListsBeforeTheAnswerAndARefusalRestoresThem() async throws {
    for refusal in [false, true] {
      let case_ = refusal ? "refused" : "answered"
      let transport = MockTransport()
      transport.enqueue("CreateClub", reply: clubCreated)
      transport.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3"]))
      transport.enqueue("ClubMembers", reply: membersReply(["u2", "u1", "u3"]))
      try await Self.openClubSession(transport)
      let club = try await Club.create(name: "friends")
      let joined = try Person.clubMembers(club.id, "JOINED")
      let owners = try Person.clubMembers(club.id, "OWNER_FIRST")
      try await joined.load()
      try await owners.load()
      func lists() async throws -> [[String]] { [try await joined.read().map(\.id), try await owners.read().map(\.id)] }
      #expect(try await lists() == [["u1", "u2", "u3"], ["u2", "u1", "u3"]], "\(case_): the lists before the action")

      transport.enqueueOutcome("RemoveClubMember", reply: clubAnswer("removeClubMember"), refusal: refusal, delay: 2)
      let started = Date()
      let removing = Task { try await club.removeMember(userIds: ["u2"]) }
      await eventually("remove member changed both lists") { ((try? await lists()) ?? []) == [["u1", "u3"], ["u1", "u3"]] }
      #expect(Date().timeIntervalSince(started) < 1.5, "\(case_): the lists changed only after the answer")
      let removed = await removing.result
      #expect(removed.isFailure == refusal, "\(case_): the outcome of remove member")
      let afterRemove = refusal ? [["u1", "u2", "u3"], ["u2", "u1", "u3"]] : [["u1", "u3"], ["u1", "u3"]]
      await eventually("the lists after remove member") { ((try? await lists()) ?? []) == afterRemove }

      if !refusal {
        transport.enqueue("AddClubMember", reply: clubAnswer("addClubMember"), delay: 2)
        let adding = Task { try await club.addMember(userIds: ["u2"]) }
        await eventually("add member changed both lists") { ((try? await lists()) ?? []) == [["u1", "u3", "u2"], ["u1", "u3", "u2"]] }
        _ = await adding.result
        #expect(try await lists() == [["u1", "u3", "u2"], ["u1", "u3", "u2"]], "\(case_): the lists after add member")
      }

      transport.enqueueOutcome("LeaveClub", reply: clubAnswer("leaveClub"), refusal: refusal, delay: 2)
      let leaving = Task { try await club.leave() }
      await eventually("leave removed the club from its list") { ((try? await Club.ids([club.id]).read().map(\.id)) ?? ["?"]).isEmpty }
      let left = await leaving.result
      #expect(left.isFailure == refusal, "\(case_): the outcome of leave")
      let remaining = try await Club.ids([club.id]).read().map(\.id)
      #expect(remaining == (refusal ? [club.id] : []), "\(case_): the club list after leave")
    }
  }

  /// K12: a forward into the same chat and into another chat leaves the source row unchanged, upserts the answer row in its own chat, and the call returns the answer row.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aForwardLeavesTheSourceUnchangedUpsertsTheAnswerRowAndReturnsIt() async throws {
    let transport = MockTransport()
    transport.enqueue("SendLetter", reply: letterSent)
    try await Self.openClubSession(transport)
    let local = try await Letter.send(in: DB.account(), chatId: "c1", text: "hello", at: 1).row
    try await local.settled(action: nil)
    let sourceId = try await local.confirmedId()
    let source = try #require(try await Letter.ids([sourceId]).read().first)

    for (target, id) in [("c1", "fwd-1"), ("c2", "fwd-2")] {
      transport.enqueue("ForwardLetter", reply: forwardAnswer(id, chat: target))
      let answer = try await source.forward(targetChatId: target)
      #expect(answer.id == id && answer.chatId == target, "\(target): the call returned \(answer.id), not the answer row")
      let unchanged = try #require(try await Letter.ids([sourceId]).read().first)
      #expect(unchanged.chatId == "c1" && unchanged.text == "hello" && unchanged.at == 1, "\(target): the answer was written onto the source row")
      let upserted = try #require(try await Letter.ids([id]).read().first)
      #expect(upserted.chatId == target, "\(target): the answer row is not in its own chat")
    }
    let sameChat = try await Letter.thread(chatId: "c1").read()
    #expect(sameChat.map(\.id).contains(sourceId) && sameChat.map(\.id).contains("fwd-1"))
  }

  /// K22 (B6, C05): a forward shows a local copy in the thread of the target chat before the answer. The answer replaces the copy, a refusal removes it.
  @MainActor
  @Test(.timeLimit(.minutes(2)))
  func aForwardShowsALocalCopyInTheTargetThreadAndARefusalRemovesIt() async throws {
    for refusal in [false, true] {
      let case_ = refusal ? "refused" : "answered"
      let transport = MockTransport()
      transport.enqueue("SendLetter", reply: letterSent)
      try await Self.openClubSession(transport)
      let local = try await Letter.send(in: DB.account(), chatId: "c1", text: "hello", at: 1).row
      try await local.settled(action: nil)
      let sourceId = try await local.confirmedId()
      let source = try #require(try await Letter.ids([sourceId]).read().first)
      let thread = try Letter.thread(chatId: "c2")
      #expect(try await thread.read().isEmpty)

      transport.enqueueOutcome("ForwardLetter", reply: forwardAnswer("fwd-1", chat: "c2"), refusal: refusal, delay: 2)
      let started = Date()
      let forwarding = Task { try await source.forward(targetChatId: "c2") }
      await eventually("the target thread shows the copy") { ((try? await thread.read().count) ?? 0) == 1 }
      #expect(Date().timeIntervalSince(started) < 1.5, "\(case_): the copy appeared only after the answer")
      let copy = try #require(try await thread.read().first)
      #expect(copy.id != "fwd-1" && copy.text == "hello" && copy.chatId == "c2", "\(case_): the copy is \(copy.id)")
      let result = await forwarding.result
      #expect(result.isFailure == refusal, "\(case_): the outcome of forward")
      await eventually("the target thread shows the outcome") { ((try? await thread.read().map(\.id)) ?? ["?"]) == (refusal ? [] : ["fwd-1"]) }
    }
  }
}

private extension Result {
  var isFailure: Bool { if case .failure = self { return true } else { return false } }
}
