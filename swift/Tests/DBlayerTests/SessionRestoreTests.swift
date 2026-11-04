import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class HeldProfileTransport: Transport, @unchecked Sendable {
  let base = MockTransport()
  let arrived = AsyncStream<Void>.makeStream()
  let released = AsyncStream<Void>.makeStream()
  let ended = AsyncStream<Void>.makeStream()
  private let lock = NSLock()
  private var first = true
  func openCable(lease: CableLease) async throws { try await base.openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await base.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await base.cableNext(token: token) }
  func cableClose(token: String) async { await base.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String {
    let held = lock.withLock { let value = first; first = false; return value }
    if !held { return try await base.query(request: request) }
    defer { ended.continuation.yield(()) }
    arrived.continuation.yield(())
    _ = await Cursor(released.stream).next("the held request is released")
    return #"{"data":{"me":{"__typename":"User","id":"1","name":"A"}}}"#
  }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { try await base.subscribe(request: request, sink: sink) }
  func unsubscribe(token: String) { base.unsubscribe(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, tag: String, progress: ByteProgress) async throws -> PutReply { try await base.put(url: url, headers: headers, file: file, tag: tag, progress: progress) }
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aHeldLoadEndsAtResetAndCannotWriteIntoTheNextAccount() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = HeldProfileTransport()
    defer { transport.released.continuation.yield(()) }
    transport.base.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    let oldEpoch = try DB.database.sessionEpoch()
    let oldScope = try DB.account()
    let result = AsyncStream<Bool>.makeStream()
    Task {
      do { try await User.profile.load(in: oldScope); result.continuation.yield(false) }
      catch DbError.Interrupted { result.continuation.yield(true) }
      catch { result.continuation.yield(false) }
    }
    _ = try #require(await Cursor(transport.arrived.stream).next("the old request"))
    try await DB.sessionReset()
    do {
      try await DB.database.resume(epoch: oldEpoch)
      Issue.record("the old resume was accepted")
    } catch DbError.Interrupted {}
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
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    for name in ["A", "B"] {
      transport.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"1","name":"\#(name)"}}}"#)
    }
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    try await User.profile.load(in: DB.account())
    let old = try #require(try await User.profile.read(in: DB.account()).first)
    try await old.set(typing: true, in: DB.account())
    #expect(try await User.profile.read(in: DB.account()).first?.typing == true)

    try await DB.sessionReset()
    try await User.profile.load(in: DB.account())
    let current = try #require(try await User.profile.read(in: DB.account()).first)

    #expect(current.id == old.id && current.name == "B" && !current.typing)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theLoadThatRestoresAResetSessionReturnsItsOwnResult() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    for (user, region) in [("u1", "r1"), ("u2", "r2")] {
      transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"\#(user)","name":"\#(user)"}}}"#)
      transport.enqueue("CurrentRegionQuery", reply: #"{"data":{"region":{"id":"\#(region)","name":"\#(region)"}}}"#)
    }
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"CurrentRegionQuery","document":"query CurrentRegionQuery { region { id name } }"}]"#)
    try await CurrentUser.current.load(in: DB.account())
    try await CurrentRegion.current.load(in: DB.account())
    #expect(try await CurrentUser.current.read(in: DB.account()).map(\.id) == ["u1"])

    try await DB.sessionReset()
    try await CurrentUser.current.load(in: DB.account())
    try await CurrentRegion.current.load(in: DB.account())

    #expect(transport.calls.count == 4)
    #expect(try await CurrentUser.current.read(in: DB.account()).map(\.id) == ["u2"])
    #expect(try await CurrentRegion.current.read(in: DB.account()).map(\.id) == ["r2"])
  }
}
