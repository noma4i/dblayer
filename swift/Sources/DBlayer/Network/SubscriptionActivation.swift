import Foundation

public struct SessionState: Sendable, Equatable {
  public let preloadFinished: Bool
  public let isAuthenticated: Bool
  public let hasPendingAuthRoute: Bool
  public let registrationCompleted: Bool
}

public enum SubscriptionActivation: Sendable {
  public static func isActive(_ state: SessionState) -> Bool {
    state.preloadFinished && state.isAuthenticated && !state.hasPendingAuthRoute && state.registrationCompleted
  }

  public static func blockingReason(_ state: SessionState) -> String? {
    if !state.preloadFinished {
      return "preload"
    }
    if !state.isAuthenticated {
      return "auth"
    }
    if state.hasPendingAuthRoute {
      return "auth-route"
    }
    if !state.registrationCompleted {
      return "registration"
    }
    return nil
  }

  public static func transition(from previous: Bool, to next: Bool) -> String {
    if !previous, next {
      return "subscribe"
    }
    if previous, !next {
      return "unsubscribe"
    }
    return "keep"
  }

  public static func documentsToOpen(state: SessionState, catalog: [String]) -> [String] {
    guard isActive(state) else {
      return []
    }
    return catalog
  }
}
