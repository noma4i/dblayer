public enum SubscriptionFailureKind: String, Sendable, CaseIterable, Equatable {
  case transport
  case graphql
  case internalError = "internal"
}
