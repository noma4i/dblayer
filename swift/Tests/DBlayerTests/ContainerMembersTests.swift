import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let membersDocument = "query ContainerMembers($containerId: ID!, $first: Int, $after: Cursor) { node(id: $containerId) { ... on Container { membersCount members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

  private func membersPage(_ id: String, hasNext: Bool) -> String {
    #"{"data":{"node":{"membersCount":53,"members":{"edges":[{"cursor":"\#(id)","node":{"id":"\#(id)","name":"Member"}}],"pageInfo":{"hasNextPage":\#(hasNext),"endCursor":"\#(id)"}}}}}"#
  }

  @Test(.timeLimit(.minutes(1)))
  func containerMembersPassesArgumentsAndDecodesTypedSnapshots() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    transport.enqueue("ContainerMembers", reply: membersPage("a0", hasNext: true))
    transport.enqueue("ContainerMembers", reply: membersPage("a1", hasNext: false))

    let first = try User.containerMembers("c1")
    try await first.load()
    let counted = try await Cursor(await first.snapshots.stream()).first("the member count") { $0.total == 53 }
    let total: Int? = counted?.total
    #expect(total == 53)
    #expect(counted?.rows.map(\.id) == ["a0"])
    #expect(counted?.rows.first?.name == "Member")
    #expect(counted?.hasMore == true)
    #expect(try await first.loadMore() == .loaded(hasMore: false))
    let calls = transport.calls.filter { $0.operationName == "ContainerMembers" }
    let variables = try calls.map { try JSONDecoder().decode(JSONValue.self, from: Data($0.variablesJson.utf8)) }
    #expect(variables == [
      .object(["containerId": .string("c1"), "first": .integer(30)]),
      .object(["containerId": .string("c1"), "first": .integer(30), "after": .string("a0")]),
    ])
  }

  @Test
  func loadAllReadsEveryPageAndForceRestartsTheList() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    transport.enqueue("ContainerMembers", reply: membersPage("a0", hasNext: true))
    transport.enqueue("ContainerMembers", reply: membersPage("a1", hasNext: false))
    transport.enqueue("ContainerMembers", reply: membersPage("a2", hasNext: false))

    let members = try User.containerMembers("c1")
    try await members.loadAll()
    try await members.loadAll(force: true)
    let calls = transport.calls.filter { $0.operationName == "ContainerMembers" }
    let variables = try calls.map { try JSONDecoder().decode(JSONValue.self, from: Data($0.variablesJson.utf8)) }
    #expect(variables == [
      .object(["containerId": .string("c1"), "first": .integer(30)]),
      .object(["containerId": .string("c1"), "first": .integer(30), "after": .string("a0")]),
      .object(["containerId": .string("c1"), "first": .integer(30)]),
    ])
  }
}
