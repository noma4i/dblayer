public struct SubscriptionFailure: Sendable, Equatable {
  public let kind: SubscriptionFailureKind
  public let message: String
  public let details: String?

  public init(kind: SubscriptionFailureKind, message: String, details: String? = nil) {
    self.kind = kind
    self.message = message
    self.details = details
  }
}
