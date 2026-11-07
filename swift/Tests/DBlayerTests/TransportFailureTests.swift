import Foundation
import Testing
@testable import DBlayer

struct TransportFailureTests {
  @Test(arguments: [URLError.Code.secureConnectionFailed, .serverCertificateHasBadDate, .serverCertificateUntrusted, .serverCertificateHasUnknownRoot, .serverCertificateNotYetValid, .clientCertificateRejected, .clientCertificateRequired])
  func encryptedHttpTrustFailuresKeepTheirSecurityCode(code: URLError.Code) {
    let error = URLError(code)
    #expect(TransportFailure.classifyEncryptedHttp(error) == .Security(code: .tlsTrust, detail: error.localizedDescription))
  }

  @Test(arguments: [SessionCode.unauthenticated, .accountUnavailable, .sessionEnded])
  func sessionFailureKeepsItsCodeAndDetail(code: SessionCode) {
    let error = DbError.Session(code: code, detail: "session_unknown")
    for kind in [GraphQLOperationKind.query, .mutation] {
      guard case let .Session(actual, detail) = TransportFailure.classify(error, kind: kind) else {
        Issue.record("session failure lost its type")
        continue
      }
      #expect(actual == code)
      #expect(detail == "session_unknown")
    }
    guard case let .Session(actual, detail) = TransportFailure.classifySubscription(error) else {
      Issue.record("subscription failure lost its session code")
      return
    }
    #expect(actual == code)
    #expect(detail == "session_unknown")
  }

  struct Case: Sendable, CustomTestStringConvertible {
    let name: String
    let error: any Error & Sendable
    let kind: GraphQLOperationKind
    let expected: String

    var testDescription: String { "\(kind) \(name)" }
  }

  static let cases: [Case] = [
    Case(name: "task cancelled", error: CancellationError(), kind: .query, expected: "Interrupted"),
    Case(name: "native failure", error: CocoaError(.fileNoSuchFile), kind: .query, expected: "Network"),
    Case(name: "native failure", error: CocoaError(.fileNoSuchFile), kind: .mutation, expected: "Interrupted"),
    Case(name: "graphQL errors", error: DbError.Graphql(messages: ["too large"], extensions: nil), kind: .mutation, expected: "Graphql too large"),
    Case(name: "unauthenticated", error: DbError.Http(status: 401, detail: "refused"), kind: .mutation, expected: "Http 401"),
    Case(name: "server 503", error: DbError.Http(status: 503, detail: "busy"), kind: .mutation, expected: "Http 503"),
    Case(name: "server 503", error: DbError.Http(status: 503, detail: "busy"), kind: .query, expected: "Http 503"),
    Case(name: "not connected", error: URLError(.notConnectedToInternet), kind: .mutation, expected: "Network"),
    Case(name: "cannot connect", error: URLError(.cannotConnectToHost), kind: .mutation, expected: "Network"),
    Case(name: "timeout", error: URLError(.timedOut), kind: .mutation, expected: "Interrupted"),
    Case(name: "timeout", error: URLError(.timedOut), kind: .query, expected: "Network"),
    Case(name: "connection lost", error: URLError(.networkConnectionLost), kind: .mutation, expected: "Interrupted"),
    Case(name: "connection lost", error: URLError(.networkConnectionLost), kind: .query, expected: "Network"),
    Case(name: "cancelled", error: URLError(.cancelled), kind: .mutation, expected: "Interrupted")
  ]

  static func shape(_ error: DbError) -> String {
    switch error {
    case .Network: "Network"
    case .Interrupted: "Interrupted"
    case .Unauthorized: "Unauthorized"
    case let .Http(status, _): "Http \(status)"
    case let .Graphql(messages, _): "Graphql \(messages.joined(separator: ","))"
    default: "other"
    }
  }

  @Test(arguments: cases)
  func aFailureReachesTheCoreAsTheClassThatTellsIfTheServerAnswered(_ failure: Case) {
    #expect(Self.shape(TransportFailure.classify(failure.error, kind: failure.kind)) == failure.expected)
  }

  @Test
  func onlyTheClassifierBuildsTheRetryableNetworkError() throws {
    let sources = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
      .appendingPathComponent("Sources/DBlayer")
    let files = try #require(FileManager.default.enumerator(at: sources, includingPropertiesForKeys: nil))
    for case let file as URL in files where file.pathExtension == "swift" {
      if ["dblayer.swift", "TransportFailure.swift"].contains(file.lastPathComponent) { continue }
      let text = try String(contentsOf: file, encoding: .utf8)
      let offenders = text.split(separator: "\n").filter { $0.contains("Network(") }
      #expect(offenders.isEmpty, "\(file): \(offenders)")
    }
  }
}
