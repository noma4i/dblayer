import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

struct MockTransportStrictTests {
  private let request = TransportRequest(document: "query Ping { ping }", operationName: "Ping", variablesJson: #"{"page":2}"#, purpose: .standard, author: .window, requestId: 1)

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

  @Test(.timeLimit(.minutes(1)))
  func aRepeatOfACancelledRequestGetsTheSameReplyAndAnotherRequestTheNextOne() async throws {
    let transport = MockTransport(unanswered: .networkError)
    let held = transport.hold("Ping", reply: "first")
    transport.enqueue("Ping", reply: "second")
    let other = TransportRequest(document: "query Ping { ping }", operationName: "Ping", variablesJson: #"{"page":2}"#, purpose: .standard, author: .window, requestId: 2, attempt: 1)
    let repeated = TransportRequest(document: "query Ping { ping }", operationName: "Ping", variablesJson: #"{"page":2}"#, purpose: .standard, author: .window, requestId: 1, attempt: 2)
    let cancelled = Task { try await transport.mutate(request: request) }
    await held.entered()
    cancelled.cancel()
    _ = try? await cancelled.value
    #expect(try await transport.mutate(request: repeated) == "first")
    #expect(try await transport.mutate(request: repeated) == "first")
    #expect(try await transport.mutate(request: other) == "second")
    await #expect(throws: DbError.self) { try await transport.mutate(request: other) }
  }
}

extension BridgeProbeTests {
  @Test @MainActor
  func aLoadOfTheCoreWithoutAReplyFailsTheTestThatCausedIt() async throws {
    try await withKnownIssue {
      let transport = MockTransport()
      try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
      _ = try? await Moment.feed.load()
      #expect(!transport.unscripted.isEmpty)
    } matching: { issue in
      "\(issue.comments)".contains("no reply queued for MomentsFeed variables=")
    }
  }
}
