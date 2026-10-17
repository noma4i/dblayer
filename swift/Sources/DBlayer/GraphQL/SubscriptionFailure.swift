public struct SubscriptionFailure: Sendable, Equatable {
  public let error: DbError

  public init(error: DbError) { self.error = error }

  public var kind: SubscriptionFailureKind {
    switch error {
    case .Graphql: .graphql
    case .Network, .Http: .transport
    default: .internalError
    }
  }

  public var message: String {
    switch error {
    case let .Graphql(messages, _): messages.joined(separator: "\n")
    default: String(describing: error)
    }
  }
}
