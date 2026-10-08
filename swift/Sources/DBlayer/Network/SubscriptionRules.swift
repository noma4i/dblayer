

public enum SubscriptionLifecycle: String, Sendable, CaseIterable, Equatable {
  case requested
  case ready
  case receiving
  case failed
  case cancelled
}

public enum SubscriptionEvent: String, Sendable, CaseIterable, Equatable {
  case serverAck
  case payload
  case failure
  case cancel
}

public enum SubscriptionRules: Sendable {
  public static func next(
    from stage: SubscriptionLifecycle,
    event: SubscriptionEvent
  ) -> SubscriptionLifecycle {
    switch event {
    case .serverAck:
      stage == .requested ? .ready : stage
    case .payload:
      switch stage {
      case .ready, .receiving:
        .receiving
      default:
        stage
      }
    case .failure:
      .failed
    case .cancel:
      .cancelled
    }
  }

  public static func deliversReady(stage: SubscriptionLifecycle) -> Bool {
    stage == .ready
  }

  public static func acceptsPayload(stage: SubscriptionLifecycle) -> Bool {
    stage == .ready || stage == .receiving
  }
}
