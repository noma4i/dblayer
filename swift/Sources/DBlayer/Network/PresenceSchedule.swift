import Foundation

public enum PresenceStep: String, Sendable, CaseIterable, Equatable {
  case sendPing
  case skipPing
  case stopPing
}

public enum PresenceSchedule: Sendable {
  public static func step(isForeground: Bool, hasConsumer: Bool, isReconnecting: Bool) -> PresenceStep {
    guard isForeground else {
      return .stopPing
    }
    if !hasConsumer || isReconnecting {
      return .skipPing
    }
    return .sendPing
  }

  public static func nextPingDelay(lastPingAt: TimeInterval?, now: TimeInterval) -> TimeInterval {
    guard let lastPingAt else {
      return 0
    }
    return max(0, OnlineChannel.pingInterval - (now - lastPingAt))
  }

  public static func leaseExpired(lastPingAt: TimeInterval, now: TimeInterval) -> Bool {
    now - lastPingAt > OnlineChannel.leaseSeconds
  }

  public static func keepsLeaseOnUnsubscribe() -> Bool {
    true
  }
}
