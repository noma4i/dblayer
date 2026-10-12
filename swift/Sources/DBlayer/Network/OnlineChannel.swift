import Foundation

public enum OnlineChannel: Sendable {
  public static let channelName = "OnlineChannel"
  public static let pingInterval: TimeInterval = 30

  public enum PresenceState: String, Sendable {
    case online
    case offline
  }

  public static func identifier(userId: String) -> String {
    CableCommand.identifier(
      channel: channelName,
      parameters: [("channelId", userId)]
    )
  }

  public static func subscribe(userId: String) -> CableCommand {
    .subscribe(identifier: identifier(userId: userId))
  }

  public static func ping(userId: String, at date: Date) -> CableCommand {
    let timestamp = Int64((date.timeIntervalSince1970 * 1000).rounded(.towardZero))
    return .message(
      identifier: identifier(userId: userId),
      data: "{\"action\":\"ping\",\"timestamp\":\(timestamp)}"
    )
  }
}
