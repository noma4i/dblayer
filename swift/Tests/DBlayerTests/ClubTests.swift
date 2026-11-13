import DBlayerTesting
import Foundation
import SQLite3
import Testing
@testable import DBlayer

private let clubCreated = #"{"data":{"createClub":{"__typename":"Club","id":"g1","name":"friends"}}}"#
private let clubDefs = #"[{"name":"CreateClub","document":"mutation CreateClub($name: String!) { createClub(name: $name) { id name } }"},{"name":"LeaveClub","document":"mutation LeaveClub($clubId: ID!) { leaveClub(clubId: $clubId) { id name } }"},{"name":"AddClubMember","document":"mutation AddClubMember($clubId: ID!, $userIds: [ID!]!) { addClubMember(clubId: $clubId, userIds: $userIds) { id name } }"},{"name":"RemoveClubMember","document":"mutation RemoveClubMember($clubId: ID!, $userIds: [ID!]!) { removeClubMember(clubId: $clubId, userIds: $userIds) { id name } }"},{"name":"ClubMembers","document":"query ClubMembers($clubId: ID!, $order: String!, $first: Int, $after: Cursor) { node(id: $clubId) { ... on Club { members(first: $first, after: $after, order: $order) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"},{"name":"SendLetter","document":"mutation SendLetter($chatId: ID!, $text: String!, $at: Int!) { sendLetter(chatId: $chatId, text: $text, at: $at) { id chatId text at } }"},{"name":"ForwardLetter","document":"mutation ForwardLetter($letterId: ID!, $chatId: ID!) { forwardLetter(letterId: $letterId, chatId: $chatId) { letter { id chatId text at } } }"}]"#

func clubAnswer(_ root: String) -> String {
  #"{"data":{"\#(root)":{"__typename":"Club","id":"g1","name":"friends"}}}"#
}
func membersReply(_ ids: [String]) -> String {
  let edges = ids.map { #"{"cursor":"\#($0)","node":{"id":"\#($0)","name":"\#($0)"}}"# }.joined(separator: ",")
  return #"{"data":{"node":{"members":{"edges":[\#(edges)],"pageInfo":{"hasNextPage":false,"endCursor":"\#(ids.last ?? "")"}}}}}"#
}


private final class MemberEventsTransport: ScriptedTransport, WrapsMock, @unchecked Sendable {
  let base = MockTransport()
  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome { await base.scriptedCable(url: url, events: events) }
  func query(request: TransportRequest) async throws -> String { try await base.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    try await base.scriptedPut(url: url, headers: headers, file: file, identity: identity, progress: progress)
  }
}

private func clubModels(keep: Bool = false, complete: Bool = false, keepLetter: Bool = false, events: Bool = false, destroy: Bool = false) throws -> String {
  guard keep || complete || keepLetter || events || destroy else { return modelsJSON }
  var models = try JSONSerialization.jsonObject(with: Data(modelsArrayJSON.utf8)) as! [[String: Any]]
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
  let text = specs(String(decoding: try JSONSerialization.data(withJSONObject: models), as: UTF8.self))
  return events ? schema(onlySubscription: "MemberChanged", in: text) : text
}

extension BridgeProbeTests {

  @MainActor
  static func confirmedClub() async throws -> Club {
    let creating = Task { try await Club.create(name: "friends") }
    let receipt = try await valueBeforeDeadline(creating) {}
    let waiting = Task { try await receipt.settled() }
    guard case .confirmed = try await valueBeforeDeadline(waiting, release: {}) else { throw WaitFailure.unexpectedOutcome }
    return try #require(try await Query<Club>().read().first)
  }

  @MainActor
  static func removeMemberConfirmed(_ club: Club) async throws {
    let removing = Task { try await club.removeMember(userIds: ["u4"]) }
    let receipt = try await valueBeforeDeadline(removing) {}
    let waiting = Task { try await receipt.settled() }
    guard case .confirmed = try await valueBeforeDeadline(waiting, release: {}) else { throw WaitFailure.unexpectedOutcome }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aMemberEventKeepsTheOpenJoinInTheList() async throws {
    let transport = MemberEventsTransport()
    transport.base.enqueue("CreateClub", reply: clubCreated)
    transport.base.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3", "u4"]))
    try await Self.openClubSession(transport, models: clubModels(events: true), subscription: #"{"name":"MemberChanged","document":"subscription MemberChanged { memberChanged { id name } }"}"#)
    let club = try await Self.confirmedClub()
    let joined = try Person.clubMembers(club.id, "JOINED")
    try await joined.load()
    transport.base.enqueue("RemoveClubMember", reply: clubAnswer("removeClubMember"))
    transport.base.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3"]))
    try await Self.removeMemberConfirmed(club)
    try #require(try await joined.read().allSatisfy { $0.id != "u4" })
    let stream = try DB.account().stream(joined)
    let cursor = Cursor(stream)
    var projection = ListProjection<Person>()
    await consuming(cursor, "the first settled list", timeout: .seconds(10), until: { projection.state?.settled == true }) { _ = try? projection.apply($0) }
    try #require(projection.state?.settled == true, "the list did not settle")
    await eventually("member subscription", timeout: .seconds(10)) { transport.base.calls.contains { $0.operationName == "MemberChanged" } }
    let hold = transport.base.hold("AddClubMember", reply: clubAnswer("addClubMember"))
    defer { hold.release() }
    let adding = Task { try await club.addMember(userIds: ["u4"]) }
    try #require(await entered(hold), "join did not reach the transport")
    try transport.base.emit("MemberChanged", json: #"{"data":{"memberChanged":{"id":"u1","name":"updated"}}}"#)
    await consuming(cursor, "the member event", timeout: .seconds(10), until: { projection.rows.contains(where: { $0.id == "u1" && $0.name == "updated" }) }) { _ = try? projection.apply($0) }
    try #require(projection.rows.contains { $0.id == "u1" && $0.name == "updated" })
    #expect(projection.rows.map(\.id).contains("u4"))
    let receipt = try await valueBeforeDeadline(adding) { hold.release() }
    transport.base.enqueue("ClubMembers", reply: membersReply(["u1", "u2", "u3", "u4"]))
    hold.release()
    let outcome = Task { try await receipt.settled() }
    let result = try await valueBeforeDeadline(outcome) { hold.release() }
    if case .confirmed = result {} else { Issue.record("the join was not confirmed") }
  }

  @MainActor
  static func openClubSession(_ transport: any Transport, models: String = modelsJSON, subscription: String? = nil) async throws {
    let defs = subscription.map { String(clubDefs.dropLast()) + ",\($0)]" } ?? clubDefs
    try await openSigned(config: Config(path: nil), modelsJson: models, transport: transport, operations: defs)
  }

}
