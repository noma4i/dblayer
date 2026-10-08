import Foundation

public enum CableHeartbeat: Sendable {
  public static let staleAfter: TimeInterval = 6
  public static let reopenDelay: TimeInterval = 0.5
  public static let foregroundDelay: TimeInterval = 0.2
  public static let maximumAttempts: Int = 10

  public static func isStale(lastFrameAt: Date?, now: Date) -> Bool {
    guard let lastFrameAt else {
      return true
    }
    return now.timeIntervalSince(lastFrameAt) >= staleAfter
  }

  public static func pollInterval(attempts: Int, jitter: Double) -> TimeInterval {
    staleAfter * pow(1.15, Double(min(attempts, maximumAttempts))) * (1 + jitter)
  }

  public static func jitterBound(attempts: Int) -> Double {
    attempts == 0 ? 1 : 0.15
  }
}
