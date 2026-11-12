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
}
