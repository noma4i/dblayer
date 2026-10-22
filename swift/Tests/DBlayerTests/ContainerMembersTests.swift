import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let membersDocument = "query ContainerMembers($containerId: ID!, $first: Int, $after: Cursor) { node(id: $containerId) { ... on Container { members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

  private func membersPage(_ prefix: String, _ range: Range<Int>, hasNext: Bool) -> String {
    let edges = range.map { #"{"cursor":"\#(prefix)\#($0)","node":{"id":"\#(prefix)\#($0)","name":"x"}}"# }.joined(separator: ",")
    return #"{"data":{"node":{"members":{"edges":[\#(edges)],"pageInfo":{"hasNextPage":\#(hasNext),"endCursor":"\#(prefix)\#(range.upperBound - 1)"}}}}}"#
  }

  @Test(.timeLimit(.minutes(1)))
  func containerMembersPageThroughTheNodeAndKeepEveryContainerApart() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    transport.enqueue("ContainerMembers", reply: membersPage("a", 0..<30, hasNext: true))
    transport.enqueue("ContainerMembers", reply: membersPage("b", 0..<2, hasNext: false))
    transport.enqueue("ContainerMembers", reply: membersPage("a", 30..<45, hasNext: false))

    let first = User.containerMembers("c1")
    try await first.load()
    try await User.containerMembers("c2").load()
    #expect(try await first.loadMore() == false)
    let calls = transport.calls.filter { $0.operationName == "ContainerMembers" }
    #expect(calls.count == 3)
    #expect(calls[2].variablesJson.contains(#""after":"a29""#) && calls[2].variablesJson.contains(#""containerId":"c1""#))
    #expect(await first.live.current().map(\.id) == (0..<45).map { "a\($0)" })
    #expect(await User.containerMembers("c2").live.current().map(\.id) == ["b0", "b1"])
    try await first.load()
    #expect(transport.calls.filter { $0.operationName == "ContainerMembers" }.count == 3)
  }
}
