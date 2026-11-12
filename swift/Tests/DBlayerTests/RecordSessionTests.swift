import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let recordCreated = #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z"}}}"#
private let recordReacted = #"{"data":{"recordReact":{"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[{"body":"C","userId":"u1"}]}}}"#

/// A signed-in plain transport and a settled record: a test that edits the record starts after the server answered its create.
extension BridgeProbeTests {

  @MainActor
  static func openRecordSession(reacts: Int = 3, recordReply: String = recordCreated) async throws -> MockTransport {
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: recordReply)
    for _ in 0..<reacts { transport.enqueue("RecordReact", reply: recordReacted) }
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await registerStandOperations(DB.database, defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt reactions { body userId } } }"},{"name":"RecordReact","document":"mutation RecordReact($id: ID!, $reactionBody: String!, $userId: ID!) { recordReact(id: $id, reactionBody: $reactionBody, userId: $userId) { id containerId authorId text createdAt reactions { body userId } } }"}]"#)
    return transport
  }

  @MainActor
  static func settledRecord() async throws -> Record {
    let sending = Task { try await Record.create(containerId: "c1", authorId: "u1", text: "hello", in: DB.account()) }
    let receipt = try await valueBeforeDeadline(sending) {}
    let waiting = Task { try await receipt.settled() }
    guard case .confirmed = try await valueBeforeDeadline(waiting, release: {}) else { throw WaitFailure.unexpectedOutcome }
    return try #require(try await Query<Record>().read().first)
  }
}
