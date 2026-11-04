import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let recordCreated = #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z"}}}"#
private let recordReacted = #"{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"C","userId":"u1"}]}}}"#

/// A signed-in plain transport and a settled record: the tests of a non-durable action need the network, so the record is sent online and answered before the action starts.
extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func reactThenUndoKeepsTheOtherUsersReaction() async throws {
    let created = #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"B","userId":"u2"}]}}}"#
    let transport = try await Self.openRecordSession(reacts: 0, recordReply: created)
    let record = try await Self.settledRecord()
    try #require(record.reactions.contains { $0.userId == "u2" && $0.body == "B" })
    let firstReply = #"{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"B","userId":"u2"},{"body":"A","userId":"u1"}]}}}"#
    let secondReply = #"{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"B","userId":"u2"}]}}}"#
    let first = transport.hold("RecordReact", reply: firstReply)
    let second = transport.hold("RecordReact", reply: secondReply)
    defer { first.release(); second.release() }
    let reacting = Task { try await record.react(reactionBody: "A", userId: "u1") }
    let firstReceipt = try await valueBeforeDeadline(reacting) { first.release(); second.release() }
    try #require(await entered(first), "react did not reach the transport")
    let changed = try #require(firstReceipt.optimisticValue)
    let undoing = Task { try await changed.react(reactionBody: "", userId: "u1") }
    let secondReceipt = try await valueBeforeDeadline(undoing) { first.release(); second.release() }
    let optimistic = try #require(secondReceipt.optimisticValue)
    #expect(optimistic.reactions.map(\.userId) == ["u2"])
    first.release()
    let earlier = Task { try await firstReceipt.outcome() }
    let earlierOutcome = try await valueBeforeDeadline(earlier) { first.release(); second.release() }
    if case .confirmed = earlierOutcome {} else { Issue.record("react was not confirmed") }
    try #require(await entered(second), "undo did not reach the transport after react ended")
    second.release()
    let later = Task { try await secondReceipt.outcome() }
    let laterOutcome = try await valueBeforeDeadline(later) { first.release(); second.release() }
    if case .confirmed = laterOutcome {} else { Issue.record("undo was not confirmed") }
    let final = try #require(await Record.find(record.id).current())
    #expect(final.reactions.map(\.userId) == ["u2"])
    #expect(final.reactions.map(\.body) == ["B"])
  }

  @MainActor
  static func openRecordSession(reactDelay: TimeInterval = 0, reacts: Int = 3, recordReply: String = recordCreated) async throws -> MockTransport {
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: recordReply)
    for _ in 0..<reacts { transport.enqueue("RecordReact", reply: recordReacted, delay: reactDelay) }
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt reactions { body userId } } }"},{"name":"RecordReact","document":"mutation RecordReact($id: ID!, $reactionBody: String!, $userId: ID!) { recordReact(id: $id, reactionBody: $reactionBody, userId: $userId) { id containerId authorId text createdAt reactions { body userId } } }"}]"#)
    return transport
  }

  @MainActor
  static func settledRecord() async throws -> Record {
    let sending = Task { try await Record.send(in: DB.account(), containerId: "c1", authorId: "u1", text: "hello") }
    let receipt = try await valueBeforeDeadline(sending) {}
    let waiting = Task { try await receipt.outcome() }
    guard case .confirmed = try await valueBeforeDeadline(waiting, release: {}) else { throw WaitFailure.unexpectedOutcome }
    return try #require(try await Query<Record>().read().first)
  }

  /// K14 (B6, the order of the app): a reaction whose answer is delayed shows at once as unsent, and the answer ends it as synced. Old line: the reaction of a database with no session was refused, and no test held the answer.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aReactionWithADelayedAnswerShowsAsUnsentBeforeTheAnswerAndSyncedAfter() async throws {
    let transport = try await Self.openRecordSession(reacts: 0)
    let record = try await Self.settledRecord()
    let hold = transport.hold("RecordReact", reply: recordReacted)
    defer { hold.release() }
    let receipt = try await record.react(reactionBody: "C", userId: "u1")
    try #require(await entered(hold))
    let changed = try #require(receipt.optimisticValue)
    #expect(changed.reactions.map(\.body) == ["C"], "the reaction was not shown before the answer")
    let during = try #require(await Record.find(record.id).current())
    #expect(during.sync.state != .synced, "the row was synced before the server answered")
    #expect(during.reactions.map(\.body) == ["C"])
    hold.release()
    let waiting = Task { try await receipt.outcome() }
    guard case .confirmed = try await valueBeforeDeadline(waiting, release: { hold.release() }) else { throw WaitFailure.unexpectedOutcome }
    let after = try #require(await Record.find(record.id).current())
    #expect(after.sync.state == .synced)
    #expect(transport.calls.filter { $0.operationName == "RecordReact" }.count == 1)
  }
}
