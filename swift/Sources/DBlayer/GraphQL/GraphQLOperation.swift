public protocol GraphQLOperation: Sendable {
  associatedtype Data: Decodable & Sendable
  associatedtype Variables: Encodable & Sendable

  static var document: String { get }
  static var operationName: String { get }
}
