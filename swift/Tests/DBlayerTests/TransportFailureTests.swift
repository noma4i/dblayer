import Foundation
import Testing
@testable import DBlayer

struct TransportFailureTests {
  struct Case: Sendable, CustomTestStringConvertible {
    let name: String
    let error: any Error & Sendable
    let kind: GraphQLOperationKind
    let expected: String

    var testDescription: String { "\(kind) \(name)" }
  }

  static let cases: [Case] = [
    Case(name: "graphQL errors", error: GraphQLTransportError.graphQL(messages: ["too large"]), kind: .mutation, expected: "Graphql too large"),
    Case(name: "unauthenticated", error: GraphQLTransportError.unauthenticated, kind: .mutation, expected: "Http 401"),
    Case(name: "server 503", error: GraphQLTransportError.server(status: 503), kind: .mutation, expected: "Http 503"),
    Case(name: "server 503", error: GraphQLTransportError.server(status: 503), kind: .query, expected: "Http 503"),
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
    case let .Http(status, _): "Http \(status)"
    case let .Graphql(messages): "Graphql \(messages.joined(separator: ","))"
    default: "other"
    }
  }

  @Test(arguments: cases)
  func aFailureReachesTheCoreAsTheClassThatTellsIfTheServerAnswered(_ failure: Case) {
    #expect(Self.shape(TransportFailure.classify(failure.error, kind: failure.kind)) == failure.expected)
  }

  @Test
  func onlyTheClassifierAndTheBlobPutBuildTheRetryableNetworkError() throws {
    let sources = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
      .appendingPathComponent("Sources/DBlayer")
    let allowed = ["the subscription socket is not open", "return .Network(detail: error.localizedDescription)"]
    for file in ["Network/DatabaseTransport.swift", "UrlSessionTransport.swift"] {
      let text = try String(contentsOf: sources.appendingPathComponent(file), encoding: .utf8)
      let offenders = text.split(separator: "\n").filter { $0.contains("Network(") && !allowed.contains(where: $0.contains) }
      #expect(offenders.isEmpty, "\(file): \(offenders)")
    }
  }
}
