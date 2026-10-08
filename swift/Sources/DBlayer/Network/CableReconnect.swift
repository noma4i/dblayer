import Foundation

public enum CableReconnect: Sendable {
  public static let mergeDelay: TimeInterval = 0.1
  public static let maximumDelay: TimeInterval = 10
  public static let jitterShare: Double = 0.3

  public static func delay(attempt: Int, jitter: Double) -> TimeInterval {
    guard attempt >= 2 else {
      return 0
    }
    let baseDelay = min(maximumDelay, pow(2, Double(attempt - 2)))
    return baseDelay * (1 + jitterShare * jitter)
  }
}
