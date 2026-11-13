import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// Property P-BR-10: calls started in an order enter the owners in that order, without the caller awaiting the previous call. The observable is the order of the mutation requests at the transport.
@Suite
struct CallOrderTests {
  init() { _ = StatusProbe.status() }

  /// Requirement: 12 creates started from separate tasks on the main actor, none awaiting another, reach the transport in the order they started. Defect caught: the entry leaves the actor before the library takes the place of the call (D-419).
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func callsStartedWithoutWaitingReachTheTransportInTheOrderTheyStarted() async throws {
    let transport = MockTransport()
    transport.respondWith("CreateMoment") { _ in #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"# }
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await registerStandOperations(DB.database, defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    let account = try DB.account()
    let names = (0..<12).map { String(format: "call-%02d", $0) }

    let tasks = names.map { name in Task { _ = try await Moment.create(text: name, media: nil, in: account) } }
    for task in tasks { try await task.value }
    await eventually("every request reached the transport") { transport.calls.filter { $0.operationName == "CreateMoment" }.count == names.count }

    let sent = transport.calls.filter { $0.operationName == "CreateMoment" }.compactMap { call in names.first { call.variablesJson.contains($0) } }
    #expect(sent == names, "the requests left in another order than the calls started: \(sent)")
    await DB.closeForCleanup()
  }
}
