import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class SilentListener: QueryListener, @unchecked Sendable {
  func changed(change: LiveChange) {}
}

struct MockTransportStrictTests {
  private let request = TransportRequest(document: "query Ping { ping }", operationName: "Ping", variablesJson: #"{"page":2}"#)

  @Test
  func aCallWithoutAReplyFailsNamingTheOperationAndVariables() async {
    await withKnownIssue {
      let transport = MockTransport()
      do {
        _ = try await transport.query(request: request)
        Issue.record("the call without a reply returned")
      } catch let DbError.Invalid(detail) {
        #expect(detail.contains("Ping") && detail.contains(#"{"page":2}"#))
      } catch {
        Issue.record("wrong error \(error)")
      }
      #expect(transport.unscripted.map(\.operationName) == ["Ping"])
      #expect(transport.calls.map(\.operationName) == ["Ping"])
    } matching: { "\($0.comments)".contains("no reply queued for Ping variables=") }
  }

  @Test
  func aLoadOfTheCoreWithoutAReplyFailsTheTestThatCausedIt() async throws {
    try await withKnownIssue {
      let transport = MockTransport()
      let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: transport)
      try database.registerOperations(defsJson: #"[{"name":"documentBySlug","document":"query documentBySlug($slug: String!, $source: String!) { documentBySlug(slug: $slug, source: $source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated($slug: String!, $source: String!) { documentUpdated(slug: $slug, source: $source) { id slug } }","rootPath":"documentUpdated"}]"#)
      let live = database.liveQuery(queryJson: #"{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}"#, listener: SilentListener())
      defer { live.cancel() }
      await eventually("the load reaches the mock", timeout: .seconds(5)) { !transport.unscripted.isEmpty }
    } matching: { issue in
      "\(issue.comments)".contains("no reply queued for documentBySlug variables=")
    }
  }

  @Test
  func aCallWithoutAReplyIsANetworkErrorOnlyWhenTheTestSaysSo() async {
    let transport = MockTransport(unanswered: .networkError)
    do {
      _ = try await transport.query(request: request)
      Issue.record("the call without a reply returned")
    } catch DbError.Network {
    } catch {
      Issue.record("wrong error \(error)")
    }
    #expect(transport.unscripted.isEmpty)
  }

  @Test
  func anUnknownTokenIsRecorded() {
    let transport = MockTransport()
    transport.unsubscribe(token: "never-issued")
    #expect(transport.unknownTokens == ["never-issued"])
  }
}
