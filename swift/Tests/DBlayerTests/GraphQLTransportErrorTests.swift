import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct GraphQLTransportErrorTests {
  @Test
  func classifiesByStatusAndGraphQLMessages() {
    #expect(GraphQLTransportError.classify(status: 500, errorMessages: [], extensions: nil) == .server(status: 500))
    #expect(GraphQLTransportError.classify(status: 503, errorMessages: [], extensions: nil) == .server(status: 503))
    #expect(GraphQLTransportError.classify(status: 401, errorMessages: [], extensions: nil) == .unauthenticated)
    #expect(GraphQLTransportError.classify(status: 403, errorMessages: [], extensions: nil) == .unauthenticated)
    #expect(GraphQLTransportError.classify(status: 200, errorMessages: [], extensions: nil) == nil)
    #expect(GraphQLTransportError.classify(status: 204, errorMessages: [], extensions: nil) == nil)
    #expect(
      GraphQLTransportError.classify(status: 200, errorMessages: ["PersistedQueryNotFound"], extensions: nil)
        == .graphQL(messages: ["PersistedQueryNotFound"], extensions: nil)
    )
    #expect(
      GraphQLTransportError.classify(status: 400, errorMessages: ["A", "B"], extensions: nil)
        == .graphQL(messages: ["A", "B"], extensions: nil)
    )
  }

  @Test
  func messagesMatchV2() {
    #expect(GraphQLTransportError.unauthenticated.message == "UNAUTHENTICATED")
    #expect(
      GraphQLTransportError.payloadTransport(code: "PAYLOAD_DECRYPTION_FAILED", status: 403).message
        == "PAYLOAD_DECRYPTION_FAILED (HTTP 403)"
    )
    #expect(GraphQLTransportError.server(status: 500).message == "SERVER_ERROR (HTTP 500)")
    #expect(GraphQLTransportError.graphQL(messages: ["A", "B"], extensions: nil).message == "A, B")
  }
}
