import Foundation
import Testing
@testable import DBlayer

private final class FailureSink: SubscriptionSink, @unchecked Sendable {
  private let lock = NSLock()
  private var errors: [DbError] = []

  var failures: [DbError] { lock.withLock { errors } }
  override func failed(error: DbError) { lock.withLock { errors.append(error) } }
}

struct DatabaseTransportTests {
  @Test(arguments: [false, true])
  func relayPreservesServerErrorsAndClassifiesStreamFailures(streamFails: Bool) async {
    let sink = FailureSink(noHandle: .init())
    let (events, continuation) = AsyncThrowingStream<GraphQLSubscriptionEvent, Error>.makeStream()
    let refusal = DbError.Graphql(messages: ["refused"], extensions: nil)
    if streamFails {
      continuation.finish(throwing: CableClientError.rejected)
    } else {
      continuation.yield(.errors(refusal))
      continuation.finish()
    }

    await DatabaseTransport.relay(events: events, operationName: "Probe", sink: sink, onEvent: { _, _ in })

    let expected = streamFails
      ? [TransportFailure.classifySubscription(CableClientError.rejected)]
      : [refusal, TransportFailure.subscriptionCompleted]
    #expect(sink.failures == expected)
  }

  @Test
  func subscribingWithoutAClientFailsBeforeDeliveringEvents() async {
    let transport = DatabaseTransport(cable: { nil })
    let sink = FailureSink(noHandle: .init())
    let request = TransportRequest(document: "subscription Probe { id }", operationName: "Probe", variablesJson: "{}")

    await #expect(throws: TransportFailure.subscriptionUnavailable) {
      _ = try await transport.subscribe(request: request, sink: sink)
    }
    #expect(sink.failures.isEmpty)
  }

  @Test
  func rejectedSocketHandshakeUsesTheEncryptedEndpointAndClosedTokensStayClosed() async throws {
    let server = try BootstrapListener(responseStatus: "400 Bad Request")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let transport = DatabaseTransport(origin: origin, cablePath: "/api/cable", cable: { nil })

    await #expect(throws: DbError.self) { _ = try await transport.openCable() }
    let request = try #require(server.requests.first)
    #expect(request.hasPrefix("GET /api/cable HTTP/1.1\r\n"))
    #expect(!request.lowercased().contains("sec-websocket-protocol:"))
    await transport.cableClose(token: "closed")
    #expect(await transport.cableNext(token: "closed") == nil)
    await #expect(throws: DbError.Interrupted(detail: "cable socket is closed")) {
      try await transport.cableSend(token: "closed", text: "frame")
    }
  }
}
