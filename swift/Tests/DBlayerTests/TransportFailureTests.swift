import Foundation
import Testing
@testable import DBlayer

struct TransportFailureTests {
  struct Case: Sendable, CustomTestStringConvertible {
    let name: String
    let error: any Error & Sendable
    let domain: String
    let code: Int64?

    var testDescription: String { name }
  }

  static let cases: [Case] = [
    Case(name: "task cancelled", error: CancellationError(), domain: "Swift.CancellationError", code: nil),
    Case(name: "url cancelled", error: URLError(.cancelled), domain: NSURLErrorDomain, code: -999),
    Case(name: "native failure", error: CocoaError(.fileNoSuchFile), domain: NSCocoaErrorDomain, code: 4),
    Case(name: "not connected", error: URLError(.notConnectedToInternet), domain: NSURLErrorDomain, code: -1009),
    Case(name: "timeout", error: URLError(.timedOut), domain: NSURLErrorDomain, code: -1001),
    Case(name: "connection lost", error: URLError(.networkConnectionLost), domain: NSURLErrorDomain, code: -1005),
    Case(name: "untrusted", error: URLError(.serverCertificateUntrusted), domain: NSURLErrorDomain, code: -1202)
  ]

  @Test(arguments: cases)
  func theWrapperReportsTheDomainTheCodeAndTheTextOfThePlatformError(_ failure: Case) {
    let raw = RawFailure(failure.error)
    #expect(raw.domain == failure.domain)
    #expect(failure.code == nil || raw.code == failure.code)
    #expect(raw.message == failure.error.localizedDescription)
  }

  @Test(.timeLimit(.minutes(1)))
  func theExecutorAppliesTheRequestTimeoutOfTheCore() async throws {
    let server = try BootstrapListener(holdHeaders: true)
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let request = HttpRequest(url: origin.absoluteString + "graphql", method: "POST", headers: [:], body: "{}", connectMs: 10_000, requestMs: 300, purpose: .standard)
    let failure = try #require(rawFailure(await UrlSessionTransport().send(request: request)))
    #expect((failure.domain, failure.code) == (NSURLErrorDomain, Int64(URLError.timedOut.rawValue)))
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
