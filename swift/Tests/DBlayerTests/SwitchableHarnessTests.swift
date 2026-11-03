import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// The transport of the app harness: a target that the tests set and unset. An unset target throws the error the app harness throws.
final class SwitchableTransport: Transport, @unchecked Sendable {
  private let lock = NSLock()
  private var current: MockTransport?
  private let unset: DbError

  init(_ target: MockTransport?, unset: DbError) {
    current = target
    self.unset = unset
  }

  var target: MockTransport? {
    get { lock.withLock { current } }
    set { lock.withLock { current = newValue } }
  }

  private func resolve() throws -> MockTransport {
    guard let target else { throw unset }
    return target
  }

  func openCable(lease: CableLease) async throws { try await resolve().openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await resolve().cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await target?.cableNext(token: token) }
  func cableClose(token: String) async { await target?.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await resolve().query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await resolve().mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { try await resolve().subscribe(request: request, sink: sink) }
  func unsubscribe(token: String) { target?.unsubscribe(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, tag: String, progress: ByteProgress) async throws -> PutReply {
    try await resolve().put(url: url, headers: headers, file: file, tag: tag, progress: progress)
  }
}

extension BridgeProbeTests {
  private static func profile(_ name: String) -> String { #"{"data":{"me":{"__typename":"User","id":"1","name":"\#(name)"}}}"# }

  private static let operations = #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#

  /// C01, C02, R-1, R-3: one openTesting, the target unset (Android throws Interrupted, iOS Invalid), reset, settle, the next target. The reset sends nothing to either transport and the first load of the next test succeeds with its own account.
  @MainActor
  @Test
  func aHarnessResetWithTheTargetUnsetLeavesTheNextWindowClean() async throws {
    for unset in [DbError.Interrupted(detail: "transport unavailable during reset"), DbError.Invalid(detail: "No test transport")] {
      DB.close()
      let first = MockTransport(unanswered: .networkError)
      first.enqueue("UserProfile", reply: Self.profile("A"))
      let switchable = SwitchableTransport(first, unset: unset)
      try await DB.openTesting(transport: switchable, modelsJSON: modelsJSON)
      try DB.database.registerOperations(defsJson: Self.operations)
      let entity = User.entity()
      await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
      let before = first.calls.count

      switchable.target = nil
      try await DB.resetForTesting()
      try await DB.settleForTesting()
      let second = MockTransport(unanswered: .networkError)
      second.enqueue("UserProfile", reply: Self.profile("B"))
      #expect(first.calls.count == before, "work of the reset reached the old transport")
      #expect(second.calls.isEmpty, "work of the reset reached the next transport")
      switchable.target = second
      try DB.setListening(true)

      try await entity.waitForInitialLoad()
      #expect(entity.value?.name == "B")
      #expect(second.calls.count == 1)
      if case let .failed(error) = entity.read { Issue.record("the next window inherited \(error)") }
      DB.close()
    }
  }

  /// C04, R-2, R-4: close, then open again without a transport (the iOS reopen), then with one (the Android direct open). The last open reads its own transport.
  @MainActor
  @Test
  func reopeningAfterACloseGivesACleanDatabaseWithAndWithoutATransport() async throws {
    DB.close()
    let first = MockTransport(unanswered: .networkError)
    first.enqueue("UserProfile", reply: Self.profile("A"))
    try await DB.openTesting(transport: first, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: Self.operations)
    let entity = User.entity()
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    DB.close()

    try await DB.openTesting(transport: nil, modelsJSON: modelsJSON)
    DB.close()

    let second = MockTransport(unanswered: .networkError)
    second.enqueue("UserProfile", reply: Self.profile("B"))
    try await DB.openTesting(transport: second, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: Self.operations)
    let reopened = User.entity()
    await eventually(#"reopened.value?.name == "B""#) { reopened.value?.name == "B" }
    #expect(first.calls.count == 1)
    DB.close()
  }
}
