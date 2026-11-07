import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

struct MockTransportStrictTests {
  private let request = TransportRequest(document: "query Ping { ping }", operationName: "Ping", variablesJson: #"{"page":2}"#, purpose: .standard)

  @Test(.timeLimit(.minutes(1)))
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
}

extension BridgeProbeTests {
  @Test @MainActor
  func aLoadOfTheCoreWithoutAReplyFailsTheTestThatCausedIt() async throws {
    try await withKnownIssue {
      let transport = MockTransport()
      try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
      _ = try? await Moment.feed.load()
      #expect(!transport.unscripted.isEmpty)
    } matching: { issue in
      "\(issue.comments)".contains("no reply queued for MomentsFeed variables=")
    }
  }
}
