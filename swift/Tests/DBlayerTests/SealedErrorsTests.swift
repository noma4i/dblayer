import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let sessionCodes: [SessionCode] = [
  .sequenceConflict, .replayedRequest, .rateLimited, .accountUnavailable, .unauthenticated, .sessionEnded,
  .invalidCode, .invalidPhone, .invalidProvider, .providerRejected, .alreadyAuthenticated, .stateLost
]

private let operations = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"},{"name":"QrCodeCreate","document":"mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }"}]"#

private final class RefusedRetry: Database, @unchecked Sendable {
  override func sessionEpoch() -> UInt64 { 1 }
  override func retry(epoch: UInt64, model: String, rowId: String, action: String?) async throws -> WireOperationReceipt { throw DbError.Invalid(detail: "retry refused") }
  override func retryAll(epoch: UInt64) async throws -> UInt64 { throw DbError.Invalid(detail: "retry refused") }
}

extension BridgeProbeTests {
  @Test @MainActor func modelRetriesPropagateErrorsToTheirCaller() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON)
    let real = try DB.database
    defer { DB.current = real }
    let row = try await Moment.send(in: DB.account(), text: "saved").optimisticValue!
    DB.current = RefusedRetry(noHandle: .init())
    await #expect(throws: DbError.self) { try await row.retry(action: nil) }
    await #expect(throws: DbError.self) { try await DB.retryAll() }
  }
  @MainActor
  private func openSealedRig(_ transport: MockTransport) async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: operations)
  }

  private func sealedCode(_ error: Error, _ comment: String) -> SessionCode? {
    guard case DbError.Session(let code, _) = error else {
      Issue.record("\(comment): surfaced as \(error), not as a session error")
      return nil
    }
    return code
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: sessionCodes)
  func aSessionCodeArrivesTypedThroughALoadAnOperationCallAndAMutationOutcome(_ code: SessionCode) async throws {
    let transport = MockTransport()
    try await openSealedRig(transport)
    let failure = DbError.Session(code: code, detail: "\(code)")

    transport.enqueue("MomentsFeed", failure: failure)
    do {
      try await Moment.feed.load()
      Issue.record("\(code): the load did not throw")
    } catch {
      #expect(sealedCode(error, "load \(code)") == code)
    }

    transport.enqueue("QrCodeCreate", failure: failure)
    do {
      _ = try await Operations.qrCodeCreate(kind: "container", size: 3)
      Issue.record("\(code): the operation call did not throw")
    } catch {
      #expect(sealedCode(error, "call \(code)") == code)
    }

    transport.enqueue("CreateMoment", failure: failure)
    let sent = try await Moment.send(in: DB.account(), text: "hello", media: nil)
    #expect(try await sent.refusal().sessionCode == code, "outcome \(code)")
  }
}
