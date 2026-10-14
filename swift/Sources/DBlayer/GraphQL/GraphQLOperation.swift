public protocol GraphQLOperation: Sendable {
  associatedtype Data: Decodable & Sendable = Never
  associatedtype Variables: Encodable & Sendable

  static var document: String { get }
  static var operationName: String { get }
}
