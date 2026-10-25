import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let membersDocument = "query ContainerMembers($containerId: ID!, $first: Int, $after: Cursor) { node(id: $containerId) { ... on Container { membersCount members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

  private func membersPage(_ prefix: String, _ range: Range<Int>, hasNext: Bool, count: Int? = nil) -> String {
    let edges = range.map { #"{"cursor":"\#(prefix)\#($0)","node":{"id":"\#(prefix)\#($0)","name":"x"}}"# }.joined(separator: ",")
    let counted = count.map { #""membersCount":\#($0),"# } ?? ""
    return #"{"data":{"node":{\#(counted)"members":{"edges":[\#(edges)],"pageInfo":{"hasNextPage":\#(hasNext),"endCursor":"\#(prefix)\#(range.upperBound - 1)"}}}}}"#
  }

  @Test(.timeLimit(.minutes(1)))
  func containerMembersPageThroughTheNodeKeepEveryContainerApartAndTakeTheMemberCountAsTotal() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.membersDocument)])]).text)
    transport.enqueue("ContainerMembers", reply: membersPage("a", 0..<30, hasNext: true, count: 53))
    transport.enqueue("ContainerMembers", reply: membersPage("b", 0..<2, hasNext: false))
    transport.enqueue("ContainerMembers", reply: membersPage("a", 30..<45, hasNext: false))

    let first = try User.containerMembers("c1")
    try await first.load()
    try await User.containerMembers("c2").load()
    let counted = try await Cursor(await first.snapshots.stream()).first("the member count") { $0.total == 53 }
    let total: Int? = counted?.total
    #expect(total == 53)
    #expect(try await first.loadMore() == .loaded(hasMore: false))
    let calls = transport.calls.filter { $0.operationName == "ContainerMembers" }
    #expect(calls.count == 3)
    #expect(calls[2].variablesJson.contains(#""after":"a29""#) && calls[2].variablesJson.contains(#""containerId":"c1""#))
    #expect(try await first.live.current().map(\.id) == (0..<45).map { "a\($0)" })
    #expect(try await User.containerMembers("c2").live.current().map(\.id) == ["b0", "b1"])
    #expect(try await Cursor(await first.snapshots.stream()).first("the member count kept") { $0.total == 53 && !$0.hasMore } != nil)
    try await first.load()
    #expect(transport.calls.filter { $0.operationName == "ContainerMembers" }.count == 3)
  }
}
