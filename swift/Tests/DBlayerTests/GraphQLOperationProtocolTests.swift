@testable import DBlayer
import Foundation
import Testing

private struct PingQuery: GraphQLQuery {
  struct Data: Decodable, Sendable {
    let ping: String
  }

  struct Variables: Encodable, Sendable {}

  static let document = "query Ping { ping }"
  static let operationName = "Ping"
}

private struct RenameMutation: GraphQLMutation {
  struct Data: Decodable, Sendable {
    let rename: String
  }

  struct Variables: Encodable, Sendable {}

  static let document = "mutation Rename { rename }"
  static let operationName = "Rename"
}

private struct TickSubscription: GraphQLSubscription {
  struct Data: Decodable, Sendable {
    let tick: String
  }

  struct Variables: Encodable, Sendable {}

  static let document = "subscription Tick { tick }"
  static let operationName = "Tick"
}

@Suite(.serialized)
struct GraphQLOperationProtocolTests {
  @Test
  func dataDecodesPingResponse() throws {
    let data = try JSONDecoder().decode(
      PingQuery.Data.self,
      from: Data(#"{"ping":"pong"}"#.utf8)
    )

    #expect(data.ping == "pong")
  }
}
