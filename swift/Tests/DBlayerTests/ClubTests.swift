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

  /// Opens a session with a club whose two member lists hold u1 u2 u3 u4 (JOINED) and u2 u1 u3 u4 (OWNER_FIRST).
  @MainActor
  static func openClubWithMembers(_ transport: MockTransport) async throws -> (Club, Query<Person>, Query<Person>) {
    transport.enqueue("CreateClub", reply: clubCreated)
    transport.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3", "u4"]))
    transport.enqueue("ClubMembers", reply: membersReply(["u2", "u1", "u3", "u4"]))
    try await openClubSession(transport)
    let club = try await Club.create(name: "friends")
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
      _ = try await club.removeMember(userIds: ["u4"])
      #expect(try await lists() == [["u1", "u2", "u3"], ["u2", "u1", "u3"]], "\(case_): the setup")
    }
    let user = removing ? "u2" : "u4"
    let operation = removing ? "RemoveClubMember" : "AddClubMember"
    let answer = clubAnswer(removing ? "removeClubMember" : "addClubMember")
    let hold = transport.hold(operation, reply: answer, failure: refusal ? forbidden : nil)
    let acting = Task { removing ? try await club.removeMember(userIds: [user]) : try await club.addMember(userIds: [user]) }
    await hold.entered()
    let during = removing ? [["u1", "u3", "u4"], ["u1", "u3", "u4"]] : original
    #expect(try await lists() == during, "\(case_): the lists before the answer")
    hold.release()
    let outcome = await acting.result
    #expect(outcome.isFailure == refusal, "\(case_): the outcome")
    let after = refusal ? (removing ? original : [["u1", "u2", "u3"], ["u2", "u1", "u3"]]) : during
    await eventually("the lists after the outcome") { ((try? await lists()) ?? []) == after }
  }

  /// K13 (B6, C05): leave removes the club from its list before the answer. The answer keeps it away, a refusal puts it back.
  @MainActor
  @Test(.timeLimit(.minutes(2)), arguments: [false, true])
  func leaveRemovesTheClubFromItsListBeforeTheAnswerAndARefusalRestoresIt(refusal: Bool) async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated)
    try await Self.openClubSession(transport)
    let club = try await Club.create(name: "friends")
    let hold = transport.hold("LeaveClub", reply: clubAnswer("leaveClub"), failure: refusal ? forbidden : nil)
    let leaving = Task { try await club.leave() }
    await hold.entered()
    #expect(try await Club.ids([club.id]).read().isEmpty, "the club stayed in its list before the answer")
    hold.release()
    let outcome = await leaving.result
    #expect(outcome.isFailure == refusal, "the outcome of leave")
    await eventually("the club list after the outcome") { ((try? await Club.ids([club.id]).read().map(\.id)) ?? ["?"]) == (refusal ? [club.id] : []) }
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
    let local = try await Letter.send(in: DB.account(), chatId: "c1", text: "hello", at: 1).row
    try await local.settled(action: nil)
    let sourceId = try await local.confirmedId()
    let source = try #require(try await Letter.ids([sourceId]).read().first)
    let thread = try Letter.thread("c2")
    #expect(try await thread.read().isEmpty)

    let hold = transport.hold("ForwardLetter", reply: forwardAnswer("fwd-1", chat: "c2"), failure: refusal ? forbidden : nil)
    let forwarding = Task { try await source.forward(targetChatId: "c2") }
    await hold.entered()
    let copy = try #require(try await thread.read().first, "the target thread has no copy before the answer")
    #expect(copy.id != "fwd-1" && copy.text == "hello" && copy.chatId == "c2", "the copy is \(copy.id)")
    hold.release()
    let result = await forwarding.result
    #expect(result.isFailure == refusal, "the outcome of forward")
    await eventually("the target thread shows the outcome") { ((try? await thread.read().map(\.id)) ?? ["?"]) == (refusal ? [] : ["fwd-1"]) }
  }
}

private extension Result {
  var isFailure: Bool { if case .failure = self { return true } else { return false } }
}
