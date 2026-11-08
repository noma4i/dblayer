import DBlayerTesting
import Foundation
import Testing
@_spi(Testing) @testable import DBlayer

private let sessionCodes: [SessionCode] = [
  .sequenceConflict, .replayedRequest, .rateLimited, .accountUnavailable, .unauthenticated, .sessionEnded,
  .invalidCode, .invalidPhone, .invalidProvider, .providerRejected, .alreadyAuthenticated
]

private let operations = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"},{"name":"QrCodeCreate","document":"mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }"}]"#

extension BridgeProbeTests {
  @Test @MainActor func modelRetriesPropagateErrorsToTheirCaller() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    let row = try await Moment.create(text: "saved", in: DB.account()).optimisticValue!
    try await DB.close()
    await #expect(throws: DbError.self) { try await row.retry(action: nil) }
  }
  @MainActor
  private func openSealedRig(_ transport: MockTransport) async throws {
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: operations)
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
    if code == .replayedRequest { transport.enqueue("MomentsFeed", failure: failure) }
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
    let sent = try await Moment.create(text: "hello", media: nil, in: DB.account())
    #expect(try await sent.refusal().sessionCode == code, "outcome \(code)")
  }
}
