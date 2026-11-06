import Foundation
import Testing
@testable import DBlayer

private enum HeaderProbe: GraphQLOperation {
  typealias Variables = String
  static let operationName = "HeaderProbe"
  static let document = "query HeaderProbe { __typename }"
}

private final class Token: @unchecked Sendable {
  private let lock = NSLock()
  private var current = "first"
  var value: String {
    get { lock.withLock { current } }
    set { lock.withLock { current = newValue } }
  }
}

extension BridgeProbeTests {
struct OpenHeadersTests {
  @Test @MainActor
  func openReadsTheHeadersAgainForEveryRequest() async throws {
    await DB.closeForCleanup()
    let server = try BootstrapListener(responseStatus: "400 Bad Request")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let token = Token()
    try await DB.open(endpoint: origin, headers: { ["X-Token": token.value] }, modelsJSON: "[]", operations: [HeaderProbe.self])

    _ = try? await DB.run("HeaderProbe", variables: [:])
    token.value = "second"
    _ = try? await DB.run("HeaderProbe", variables: [:])

    let tokens = server.requests.compactMap { request in
      request.components(separatedBy: "\r\n").first { $0.lowercased().hasPrefix("x-token:") }.map { $0.split(separator: ":", maxSplits: 1)[1].trimmingCharacters(in: .whitespaces) }
    }
    #expect(tokens == ["first", "second"], "tokens sent: \(tokens)")
  }
}
}
