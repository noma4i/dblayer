import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let countedMembersDocument = "query ContainerMembers($containerId: ID!, $first: Int, $after: Cursor) { node(id: $containerId) { ... on Container { membersCount members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

  private func countedPage(_ range: Range<Int>, hasNext: Bool, count: Int?) -> String {
    let edges = range.map { #"{"cursor":"u\#($0)","node":{"id":"u\#($0)","name":"x"}}"# }.joined(separator: ",")
    let counted = count.map { #""membersCount":\#($0),"# } ?? ""
    return #"{"data":{"node":{\#(counted)"members":{"edges":[\#(edges)],"pageInfo":{"hasNextPage":\#(hasNext),"endCursor":"u\#(range.upperBound - 1)"}}}}}"#
  }

  @Test(.timeLimit(.minutes(1)))
  func containerMembersTotalIsTheMemberCountOfTheContainer() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerMembers"), "document": .string(Self.countedMembersDocument)])]).text)
    transport.enqueue("ContainerMembers", reply: countedPage(0..<30, hasNext: true, count: 53))
    transport.enqueue("ContainerMembers", reply: countedPage(30..<45, hasNext: true, count: nil))

    let members = User.containerMembers("c1")
    try await members.load()
    #expect(try await Cursor(await members.snapshots.stream()).first("the member count") { $0.total == 53 } != nil)
    #expect(try await members.loadMore() == .loaded(hasMore: true))
    #expect(try await Cursor(await members.snapshots.stream()).first("the member count kept") { $0.total == 53 && $0.hasMore } != nil)
  }
}
