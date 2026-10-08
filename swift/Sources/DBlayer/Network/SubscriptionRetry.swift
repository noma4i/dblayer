import Foundation

public enum SubscriptionReport: String, Sendable, CaseIterable, Equatable {
  case silent
  case degraded = "subscription-degraded"
  case permanent = "subscription-permanent"
}

public enum SubscriptionRetry: Sendable {
  public static let baseDelay: TimeInterval = 1
  public static let maxDelay: TimeInterval = 30

  public static func delay(attempt: Int) -> TimeInterval {
    guard attempt > 1 else { return baseDelay }

    return min(maxDelay, baseDelay * pow(2.0, Double(attempt - 1)))
  }

  public static func retries(isOnline: Bool, isForeground: Bool) -> Bool {
    isOnline && isForeground
  }

  public static func isActive(
    isEnabled: Bool,
    isAuthenticated: Bool,
    hasSuccessContext: Bool,
    registrationCompleted: Bool
  ) -> Bool {
    isEnabled && isAuthenticated && !hasSuccessContext && registrationCompleted
  }

  public static func resetsAttempts(onServerAck: Bool) -> Bool {
    onServerAck
  }
}
