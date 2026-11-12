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
    _ = await nextElement(of: released.stream)
    return #"{"data":{"me":{"__typename":"User","id":"1","name":"A"}}}"#
  }
  func mutate(request: TransportRequest) async throws -> String { try await base.mutate(request: request) }
  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply { try await base.scriptedPut(url: url, headers: headers, file: file, identity: identity, progress: progress) }
}

extension BridgeProbeTests {
}
