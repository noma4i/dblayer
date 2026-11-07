import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class HeldProfileTransport: ScriptedTransport, WrapsMock, @unchecked Sendable {
  let base = MockTransport()
  let arrived = AsyncStream<Void>.makeStream()
  let released = AsyncStream<Void>.makeStream()
  let ended = AsyncStream<Void>.makeStream()
  private let lock = NSLock()
  private var first = true
  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome { await base.scriptedCable(url: url, events: events) }
  func query(request: TransportRequest) async throws -> String {
    let held = lock.withLock { let value = first; first = false; return value }
    if !held { return try await base.query(request: request) }
    defer { ended.continuation.yield(()) }
    arrived.continuation.yield(())
    _ = await Cursor(released.stream).next("the held request is released")
    return #"{"data":{"me":{"__typename":"User","id":"1","name":"A"}}}"#
  }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply { try await base.scriptedPut(url: url, headers: headers, file: file, identity: identity, progress: progress) }
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aHeldLoadEndsAtResetAndCannotWriteIntoTheNextAccount() async throws {
    await DB.closeForCleanup()
    let transport = HeldProfileTransport()
    defer { transport.released.continuation.yield(()) }
    transport.base.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    let oldScope = try DB.account()
    let result = AsyncStream<Bool>.makeStream()
    Task {
      do { try await User.profile.load(in: oldScope); result.continuation.yield(false) }
      catch DbError.Interrupted { result.continuation.yield(true) }
      catch { result.continuation.yield(false) }
    }
    _ = try #require(await Cursor(transport.arrived.stream).next("the old request"))
    try await DB.sessionReset()
    try await loginAgain()
    #expect(await Cursor(result.stream).next("the old load ends") == true)
    try await User.profile.load(in: DB.account())
    transport.released.continuation.yield(())
    _ = try #require(await Cursor(transport.ended.stream).next("the old response ends"))
    #expect(try await User.profile.read(in: DB.account()).map(\.name) == ["B"])
    #expect(transport.base.calls.filter { $0.operationName == "UserProfile" }.count == 1)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func resetClearsEphemeralValuesBeforeTheNextAccountReadsTheSameId() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    for name in ["A", "B"] {
      transport.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"1","name":"\#(name)"}}}"#)
    }
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    try await User.profile.load(in: DB.account())
    let old = try #require(try await User.profile.read(in: DB.account()).first)
    try await old.set(typing: true, in: DB.account())
    #expect(try await User.profile.read(in: DB.account()).first?.typing == true)

    try await DB.sessionReset()
    try await loginAgain()
    try await User.profile.load(in: DB.account())
    let current = try #require(try await User.profile.read(in: DB.account()).first)

    #expect(current.id == old.id && current.name == "B" && !current.typing)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theLoadThatRestoresAResetSessionReturnsItsOwnResult() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    for (user, region) in [("u1", "r1"), ("u2", "r2")] {
      transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"\#(user)","name":"\#(user)"}}}"#)
      transport.enqueue("CurrentRegionQuery", reply: #"{"data":{"region":{"id":"\#(region)","name":"\#(region)"}}}"#)
    }
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"CurrentRegionQuery","document":"query CurrentRegionQuery { region { id name } }"}]"#)
    try await CurrentUser.current.load(in: DB.account())
    try await CurrentRegion.current.load(in: DB.account())
    #expect(try await CurrentUser.current.read(in: DB.account()).map(\.id) == ["u1"])

    try await DB.sessionReset()
    try await loginAgain()
    try await CurrentUser.current.load(in: DB.account())
    try await CurrentRegion.current.load(in: DB.account())

    #expect(transport.calls.count == 4)
    #expect(try await CurrentUser.current.read(in: DB.account()).map(\.id) == ["u2"])
    #expect(try await CurrentRegion.current.read(in: DB.account()).map(\.id) == ["r2"])
  }
}
