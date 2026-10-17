import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let sessionCodes: [SessionCode] = [
  .sequenceConflict, .replayedRequest, .rateLimited, .accountUnavailable, .unauthenticated, .sessionEnded,
  .invalidCode, .invalidPhone, .invalidProvider, .providerRejected, .alreadyAuthenticated
]

private let operations = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"},{"name":"QrCodeCreate","document":"mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }"}]"#

@Suite(.serialized)
struct SealedErrorsTests {
  @MainActor
  private func open(_ transport: MockTransport) throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: operations)
  }

  private func sessionCode(_ error: Error, _ comment: String) -> SessionCode? {
    guard case DbError.Session(let code, _) = error else {
      Issue.record("\(comment): surfaced as \(error), not as a session error")
      return nil
    }
    return code
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: sessionCodes)
  func aSessionCodeArrivesTypedThroughALoadAnOperationCallAndASettledMutation(_ code: SessionCode) async throws {
    let transport = MockTransport()
    try open(transport)
    let failure = DbError.Session(code: code, detail: "\(code)")

    transport.enqueue("MomentsFeed", failure: failure)
    do {
      try await Moment.feed.load()
      Issue.record("\(code): the load did not throw")
    } catch {
      #expect(sessionCode(error, "load \(code)") == code)
    }

    transport.enqueue("QrCodeCreate", failure: failure)
    do {
      _ = try await Operations.qrCodeCreate(kind: "chat", size: 3)
      Issue.record("\(code): the operation call did not throw")
    } catch {
      #expect(sessionCode(error, "call \(code)") == code)
    }

    transport.enqueue("CreateMoment", failure: failure)
    let local = try await Moment.create(text: "hello", media: nil)
    do {
      try await local.settled()
      Issue.record("\(code): settled did not throw")
    } catch {
      #expect(sessionCode(error, "settled \(code)") == code)
    }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: [SessionCode.unauthenticated, .accountUnavailable])
  func aLostSessionKeepsTheQueuedMutationFailedAndRetrySendsItOnce(_ code: SessionCode) async throws {
    let transport = MockTransport()
    try open(transport)
    transport.enqueue("CreateMoment", failure: DbError.Session(code: code, detail: "\(code)"))
    let local = try await Moment.create(text: "hello", media: nil)
    await #expect(throws: DbError.self) { try await local.settled() }
    #expect(await Moment.find(local.id).current()?.text == "hello")

    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await DB.database.retry(model: Moment.modelName, rowId: local.id)
    try await local.settled()

    #expect(transport.calls.filter { $0.operationName == "CreateMoment" }.count == 2)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anotherSessionCodeRollsTheQueuedMutationBack() async throws {
    let transport = MockTransport()
    try open(transport)
    transport.enqueue("CreateMoment", failure: DbError.Session(code: .rateLimited, detail: "RATE_LIMITED"))
    let local = try await Moment.create(text: "hello", media: nil)

    await #expect(throws: DbError.self) { try await local.settled() }

    #expect(await Moment.find(local.id).current() == nil)
  }
}
