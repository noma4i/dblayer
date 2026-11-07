import Foundation
import Testing
@testable import DBlayer

struct TransportFailureTests {
  struct Case: Sendable, CustomTestStringConvertible {
    let name: String
    let error: any Error & Sendable
    let kind: RawFailureKind

    var testDescription: String { name }
  }

  static let cases: [Case] = [
    Case(name: "task cancelled", error: CancellationError(), kind: .cancelled),
    Case(name: "url cancelled", error: URLError(.cancelled), kind: .cancelled),
    Case(name: "native failure", error: CocoaError(.fileNoSuchFile), kind: .io),
    Case(name: "not connected", error: URLError(.notConnectedToInternet), kind: .unreachable),
    Case(name: "cannot find host", error: URLError(.cannotFindHost), kind: .unreachable),
    Case(name: "cannot connect", error: URLError(.cannotConnectToHost), kind: .unreachable),
    Case(name: "dns lookup", error: URLError(.dnsLookupFailed), kind: .unreachable),
    Case(name: "data not allowed", error: URLError(.dataNotAllowed), kind: .unreachable),
    Case(name: "roaming off", error: URLError(.internationalRoamingOff), kind: .unreachable),
    Case(name: "timeout", error: URLError(.timedOut), kind: .timeout),
    Case(name: "connection lost", error: URLError(.networkConnectionLost), kind: .io),
    Case(name: "secure connection", error: URLError(.secureConnectionFailed), kind: .tls),
    Case(name: "bad date", error: URLError(.serverCertificateHasBadDate), kind: .tls),
    Case(name: "untrusted", error: URLError(.serverCertificateUntrusted), kind: .tls),
    Case(name: "unknown root", error: URLError(.serverCertificateHasUnknownRoot), kind: .tls),
    Case(name: "not yet valid", error: URLError(.serverCertificateNotYetValid), kind: .tls),
    Case(name: "client rejected", error: URLError(.clientCertificateRejected), kind: .tls),
    Case(name: "client required", error: URLError(.clientCertificateRequired), kind: .tls)
  ]

  @Test(arguments: cases)
  func aPlatformErrorMapsToTheKindItShows(_ failure: Case) {
    let raw = RawFailure(failure.error)
    #expect(raw.kind == failure.kind)
    #expect(raw.code == (failure.error as? URLError).map { Int64($0.code.rawValue) })
  }

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

  @Test
  func theWrapperNeverBuildsAClassOfItsOwn() throws {
    let sources = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
      .appendingPathComponent("Sources/DBlayer")
    let files = try #require(FileManager.default.enumerator(at: sources, includingPropertiesForKeys: nil))
    for case let file as URL in files where file.pathExtension == "swift" {
      if file.lastPathComponent == "dblayer.swift" { continue }
      let text = try String(contentsOf: file, encoding: .utf8)
      let offenders = text.split(separator: "\n").filter { $0.contains("Network(") || $0.contains("Interrupted(") || $0.contains("DbError.Http") }
      #expect(offenders.isEmpty, "\(file): \(offenders)")
    }
  }
}
