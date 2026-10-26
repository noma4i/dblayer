import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test
  func blockingJoinsTheBlockedListAtOnceAndARefusalTakesBackOnlyThatMember() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"GroupRoster","document":"query GroupRoster($groupId: ID!) { node(id: $groupId) { members { id name } } }"},{"name":"MemberBlock","document":"mutation MemberBlock($input: MemberBlockInput!) { memberBlock(input: $input) { ok } }"}]"#)
    transport.enqueue("GroupRoster", reply: #"{"data":{"node":{"members":[{"__typename":"Member","id":"m1","name":"a"},{"__typename":"Member","id":"m2","name":"b"}]}}}"#)
    try await Member.roster("g1").load()
    let first = try #require(await Member.find("m1").current())
    let second = try #require(await Member.find("m2").current())
    try await Member.blocked.warm()

    transport.enqueue("MemberBlock", reply: #"{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}"#, delay: 0.8)
    transport.enqueue("MemberBlock", reply: #"{"data":{"memberBlock":{"ok":true}}}"#, delay: 0.8)
    _ = try await first.block()
    _ = try await second.block()
    await eventually("both members join the blocked list before any answer") { (try? await Member.blocked.cached()?.value.map(\.id)) == ["m2", "m1"] }

    await eventually("the refused block is taken back") { (try? await Member.blocked.cached()?.value.map(\.id)) == ["m2"] }
    try await second.settled(action: nil)
    #expect(try await Member.blocked.cached()?.value.map(\.id) == ["m2"])
  }
}
