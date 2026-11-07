import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// The transport of the app harness: a target that the tests set and unset. An unset target throws the error the app harness throws.
final class SwitchableTransport: ScriptedTransport, WrapsMock, @unchecked Sendable {
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

  var base: MockTransport {
    guard let target else { preconditionFailure("the stand needs a target to attach its cable") }
    return target
  }

  private func resolve() throws -> MockTransport {
    guard let target else { throw unset }
    return target
  }

  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome {
    guard let target = try? resolve() else { return .failed(failure: .scripted(.cancelled, "\(unset)"), handshakeStatus: nil) }
    return await target.scriptedCable(url: url, events: events)
  }
  func query(request: TransportRequest) async throws -> String { try await resolve().query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await resolve().mutate(request: request) }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    try await resolve().scriptedPut(url: url, headers: headers, file: file, identity: identity, progress: progress)
  }
}

extension BridgeProbeTests {
  private static func profile(_ name: String) -> String { #"{"data":{"me":{"__typename":"User","id":"1","name":"\#(name)"}}}"# }

  private static let operations = #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#

  /// C01, C02, R-1, R-3: the target unset (Android throws Interrupted, iOS Invalid), the database closes, the next test opens a new one on the next target. The close sends nothing to either transport and the first load of the next test succeeds with its own account.
  @MainActor
  @Test
  func aCloseWithTheTargetUnsetLeavesTheNextDatabaseClean() async throws {
    for unset in [DbError.Interrupted(detail: "transport unavailable during reset"), DbError.Invalid(detail: "No test transport")] {
      await DB.closeForCleanup()
      let first = MockTransport(unanswered: .networkError)
      first.enqueue("UserProfile", reply: Self.profile("A"))
      let switchable = SwitchableTransport(first, unset: unset)
      try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: switchable, operations: Self.operations)
      let entity = Projected<User>(try DB.account().stream(User.profile))
      await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
      let before = first.calls.count

      switchable.target = nil
      await DB.closeForCleanup()
      let second = MockTransport(unanswered: .networkError)
      second.enqueue("UserProfile", reply: Self.profile("B"))
      #expect(first.calls.count == before, "work of the close reached the old transport: \(first.calls.dropFirst(before).map { "\(-e.author) \(-e.operationName) \(-e.requestId)" })")
      #expect(second.calls.isEmpty, "work of the close reached the next transport")
      switchable.target = second
      try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: switchable, operations: Self.operations)

      let next = Projected<User>(try DB.account().stream(User.profile))
      try await next.loaded()
      #expect(next.value?.name == "B")
      #expect(second.calls.filter { $0.operationName == "UserProfile" }.count == 1)
      await DB.closeForCleanup()
    }
  }

  /// C04, R-2, R-4: close, then open again without a transport (the iOS reopen), then with one (the Android direct open). The last open reads its own transport.
  @MainActor
  @Test
  func reopeningAfterACloseGivesACleanDatabaseWithAndWithoutATransport() async throws {
    await DB.closeForCleanup()
    let first = MockTransport(unanswered: .networkError)
    first.enqueue("UserProfile", reply: Self.profile("A"))
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: first, operations: Self.operations)
    let entity = Projected<User>(try DB.account().stream(User.profile))
    await eventually(#"entity.value?.name == "A""#) { entity.value?.name == "A" }
    await DB.closeForCleanup()
    try await DB.openTesting(transport: nil, modelsJSON: modelsJSON)
    await DB.closeForCleanup()
    let second = MockTransport(unanswered: .networkError)
    second.enqueue("UserProfile", reply: Self.profile("B"))
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: second, operations: Self.operations)
    let reopened = Projected<User>(try DB.account().stream(User.profile))
    await eventually(#"reopened.value?.name == "B""#) { reopened.value?.name == "B" }
    #expect(first.calls.filter { $0.operationName == "UserProfile" }.count == 1)
    await DB.closeForCleanup()
  }
}
