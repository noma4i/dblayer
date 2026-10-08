import DBlayer
import Foundation
import Testing
@testable import DBlayer

struct GraphQLChannelTests {
  @Test
  func commandsCarryServerWireFormat() {
    #expect(
      GraphQLChannel.identifier(channelId: "abc")
        == #"{"channel":"GraphqlChannel","channelId":"abc","api_version":"2"}"#
    )
    #expect(GraphQLChannel.requestId(channelId: "abc", attempt: 1) == "abc:1")
    #expect(GraphQLChannel.requestId(channelId: "abc", attempt: 2) == "abc:2")
    #expect(
      GraphQLChannel.execute(
        channelId: "abc",
        attempt: 1,
        document: "subscription X { x }",
        operationName: "X"
      ) == CableCommand.message(
        identifier: GraphQLChannel.identifier(channelId: "abc"),
        data: [
          "{\"action\":\"execute\",\"requestId\":\"abc:1\",\"query\":\"subscription X { x }\",",
          "\"variables\":{},\"operationName\":\"X\"}"
        ].joined()
      )
    )
  }

  @Test
  func serverFramesBecomeChannelEvents() throws {
    let ready = try decode(
      #"{"result":{"data":null},"more":true,"requestId":"abc:1","subscriptionReady":true}"#
    )
    let payload = try decode(#"{"result":{"data":{"chatUpdated":{"id":"1"}}},"more":true}"#)
    let errors = try decode(#"{"result":{"errors":[{"message":"boom"}]},"more":true}"#)
    let completed = try decode(#"{"result":{"data":null},"more":false}"#)
    let pong = try decode(#"{"type":"pong"}"#)
    let emptyData = try decode(#"{"result":{"data":{}},"more":true}"#)

    #expect(GraphQLChannel.event(from: completed, requestId: "abc:1") == .completed)
    #expect(GraphQLChannel.event(from: ready, requestId: "abc:1") == .ready)
    #expect(GraphQLChannel.event(from: ready, requestId: "abc:2") == nil)
    #expect(
      GraphQLChannel.event(from: payload, requestId: "abc:1")
        == .payload(JSONValue.object(["chatUpdated": .object(["id": .string("1")])]))
    )
    #expect(GraphQLChannel.event(from: errors, requestId: "abc:1") == .errors(["boom"]))
    #expect(GraphQLChannel.event(from: pong, requestId: "abc:1") == nil)
    #expect(GraphQLChannel.event(from: emptyData, requestId: "abc:1") == nil)
  }

  private func decode(_ json: String) throws -> JSONValue {
    try JSONDecoder().decode(JSONValue.self, from: Data(json.utf8))
  }
}
