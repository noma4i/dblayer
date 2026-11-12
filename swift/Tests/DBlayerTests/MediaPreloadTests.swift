import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private enum MediaPresenceOnline: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "PresenceOnline"
  static let document = "query PresenceOnline { presenceOnline { id userId room } }"
}

private enum ChatMediaOperation: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "ChatMedia"
  static let document = "query ChatMedia($chatId: ID!, $first: Int, $after: Cursor, $mediaKinds: [String!], $includeGifts: Boolean!) { node(id: $chatId) { ... on Chat { messages(first: $first, after: $after, mediaKinds: $mediaKinds, includeGifts: $includeGifts) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } } }"
}

extension BridgeProbeTests {
}
