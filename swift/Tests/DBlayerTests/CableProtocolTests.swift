import Foundation
import Testing
@testable import DBlayer

struct CableProtocolTests {
  @Test
  func decodesServerFrames() {
    #expect(CableFrame(json: "{\"type\":\"welcome\"}") == .welcome)
    #expect(
      CableFrame(json: "{\"type\":\"ping\",\"message\":1700000000}")
        == .ping(Date(timeIntervalSince1970: 1_700_000_000))
    )
    #expect(
      CableFrame(
        json: "{\"type\":\"confirm_subscription\",\"identifier\":\"{\\\"channel\\\":\\\"GraphqlChannel\\\"}\"}"
      ) == .confirmed(identifier: "{\"channel\":\"GraphqlChannel\"}")
    )
    #expect(
      CableFrame(
        json: "{\"type\":\"reject_subscription\",\"identifier\":\"{\\\"channel\\\":\\\"GraphqlChannel\\\"}\"}"
      ) == .rejected(identifier: "{\"channel\":\"GraphqlChannel\"}")
    )
    #expect(
      CableFrame(json: "{\"type\":\"disconnect\",\"reason\":\"unauthorized\",\"reconnect\":false}")
        == .disconnect(reason: "unauthorized", reconnect: false)
    )
    #expect(
      CableFrame(json: "{\"identifier\":\"chan\",\"message\":{\"result\":{\"data\":null}}}")
        == .message(
          identifier: "chan",
          payload: .object(["result": .object(["data": .null])])
        )
    )

    #expect(CableFrame(json: "{\"type\":\"unknown\"}") == nil)
    #expect(CableFrame(json: "not json") == nil)
    #expect(CableFrame(json: "{}") == nil)

    #expect(
      CableFrame(json: "{\"type\":\"disconnect\",\"reason\":\"remote\"}")
        == .disconnect(reason: "remote", reconnect: true)
    )
  }

  @Test
  func encodesClientCommands() {
    #expect(CableCommand.subscribe(identifier: "chan").json == "{\"command\":\"subscribe\",\"identifier\":\"chan\"}")
    #expect(CableCommand.unsubscribe(identifier: "chan").json == "{\"command\":\"unsubscribe\",\"identifier\":\"chan\"}")
    #expect(
      CableCommand.message(identifier: "chan", data: "{\"action\":\"execute\"}").json
        == "{\"command\":\"message\",\"identifier\":\"chan\",\"data\":\"{\\\"action\\\":\\\"execute\\\"}\"}"
    )

    #expect(
      CableCommand.identifier(
        channel: "GraphqlChannel",
        parameters: [("channelId", "abc")]
      ) == "{\"channel\":\"GraphqlChannel\",\"channelId\":\"abc\"}"
    )
    #expect(CableCommand.identifier(channel: "OnlineChannel", parameters: []) == "{\"channel\":\"OnlineChannel\"}")
  }
}
