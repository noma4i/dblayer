
import Foundation

public enum GraphQLOperationKind: String, Sendable {
  case query
  case mutation
}

public struct GraphQLRequest<Variables: Encodable & Sendable>: Sendable {
  public let operationName: String
  public let document: String
  public let variables: Variables
  public let kind: GraphQLOperationKind

  public init(
    operationName: String,
    document: String,
    variables: Variables,
    kind: GraphQLOperationKind = .query
  ) {
    self.operationName = operationName
    self.document = document
    self.variables = variables
    self.kind = kind
  }

  public var isPublic: Bool {
    PublicOperations.names.contains(operationName)
  }
}

public enum PublicOperations {
  public static let names: Set<String> = ["AuthSignIn", "AuthConsumeLoginLink"]
}
