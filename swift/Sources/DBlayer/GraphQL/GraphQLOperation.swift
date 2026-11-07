public protocol GraphQLOperation: Sendable {
  associatedtype Data: Decodable & Sendable = Never
  associatedtype Variables: Encodable & Sendable

  static var document: String { get }
  static var operationName: String { get }
  static var returns: String? { get }
}

extension GraphQLOperation {
  public static var returns: String? { nil }
}
