import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let recordCreated = #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z"}}}"#
private let recordReacted = #"{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"C","userId":"u1"}]}}}"#

/// A signed-in plain transport and a settled record: the tests of a non-durable action need the network, so the record is sent online and answered before the action starts.
extension BridgeProbeTests {
  @MainActor
  static func openRecordSession(reactDelay: TimeInterval = 0, reacts: Int = 3) async throws -> MockTransport {
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: recordCreated)
    for _ in 0..<reacts { transport.enqueue("RecordReact", reply: recordReacted, delay: reactDelay) }
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt } }"},{"name":"RecordReact","document":"mutation RecordReact($id: ID!, $reactionBody: String!, $userId: ID!) { recordReact(id: $id, reactionBody: $reactionBody, userId: $userId) { id containerId authorId text createdAt reactions { body userId } } }"}]"#)
    return transport
  }

  @MainActor
  static func settledRecord() async throws -> Record {
    let local = try await Record.send(in: DB.account(), containerId: "c1", authorId: "u1", text: "hello").row
    try await local.settled(action: nil)
    return try #require(await Record.find(try await local.confirmedId()).current())
  }

  /// K14 (B6, the order of the app): a reaction whose answer is delayed shows at once as unsent, and the answer ends it as synced. Old line: the reaction of a database with no session was refused, and no test held the answer.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aReactionWithADelayedAnswerShowsAsUnsentBeforeTheAnswerAndSyncedAfter() async throws {
    let transport = try await Self.openRecordSession(reactDelay: 0.6)
    let record = try await Self.settledRecord()
    let changed = try await record.react(reactionBody: "C", userId: "u1")
    #expect(changed.reactions.map(\.body) == ["C"], "the reaction was not shown before the answer")
    let during = try #require(await Record.find(record.id).current())
    #expect(during.sync.state != .synced, "the row was synced before the server answered")
    #expect(during.reactions.map(\.body) == ["C"])
    try await changed.settled(action: "react")
    let after = try #require(await Record.find(record.id).current())
    #expect(after.sync.state == .synced)
    #expect(transport.calls.filter { $0.operationName == "RecordReact" }.count == 1)
  }
}
