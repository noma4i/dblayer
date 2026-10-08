import Foundation

public struct GraphQLError: Decodable, Sendable, Equatable {
  public let message: String
  public let path: [String]?
  public let extensions: [String: JSONValue]?

  public var code: String? {
    guard case let .string(code) = extensions?["code"] else {
      return nil
    }
    return code
  }
}
