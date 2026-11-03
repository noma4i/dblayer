import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let clubCreated = #"{"data":{"createClub":{"__typename":"Club","id":"g1","name":"friends"}}}"#
private let letterSent = #"{"data":{"sendLetter":{"__typename":"Letter","id":"l1","chatId":"c1","text":"hello","at":1}}}"#
private let clubDefs = #"[{"name":"CreateClub","document":"mutation CreateClub($name: String!) { createClub(name: $name) { id name left membership } }"},{"name":"LeaveClub","document":"mutation LeaveClub($clubId: ID!) { leaveClub(clubId: $clubId) { id name left membership } }"},{"name":"AddClubMember","document":"mutation AddClubMember($clubId: ID!) { addClubMember(clubId: $clubId) { id name left membership } }"},{"name":"RemoveClubMember","document":"mutation RemoveClubMember($clubId: ID!) { removeClubMember(clubId: $clubId) { id name left membership } }"},{"name":"SendLetter","document":"mutation SendLetter($chatId: ID!, $text: String!, $at: Int!) { sendLetter(chatId: $chatId, text: $text, at: $at) { id chatId text at } }"},{"name":"ForwardLetter","document":"mutation ForwardLetter($letterId: ID!, $chatId: ID!) { forwardLetter(letterId: $letterId, chatId: $chatId) { letter { id chatId text at } } }"}]"#

private func clubAnswer(_ root: String, left: Bool, membership: String?) -> String {
  let state = membership.map { #""\#($0)""# } ?? "null"
  return #"{"data":{"\#(root)":{"__typename":"Club","id":"g1","name":"friends","left":\#(left),"membership":\#(state)}}}"#
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

  /// K13 (B6): leave, add member and remove member show their change at once as unsent, before the server answers, and end as synced after the answer.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aClubActionShowsAsUnsentBeforeTheAnswerAndSyncedAfter() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateClub", reply: clubCreated)
    try await Self.openClubSession(transport)
    let club = try await Club.create(name: "friends")
    let actions: [(String, String, String, Bool, String?)] = [("leave", "LeaveClub", "leaveClub", true, nil), ("add_member", "AddClubMember", "addClubMember", false, "added"), ("remove_member", "RemoveClubMember", "removeClubMember", false, "removed")]
    for (action, operation, root, left, membership) in actions {
      transport.enqueue(operation, reply: clubAnswer(root, left: left, membership: membership), delay: 0.6)
      let changed: Club
      switch action {
      case "leave": changed = try await club.leave()
      case "add_member": changed = try await club.addMember()
      default: changed = try await club.removeMember()
      }
      #expect(changed.sync.state != .synced, "\(action): the row was synced before the server answered")
      let shown = membership == nil ? changed.left == true : changed.membership == membership
      #expect(shown, "\(action): the change was not shown before the answer")
      try await changed.settled(action: action)
      let after = try #require(try await Club.ids([club.id]).read().first)
      #expect(after.sync.state == .synced, "\(action): the row did not end as synced")
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
}
