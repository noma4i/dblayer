import Foundation

public enum SubscriptionName: String, Sendable, CaseIterable, Equatable, Hashable {
  case chatCreated
  case chatUpdated
  case chatDeleted
  case messageCreated
  case messageUpdated
  case messageDeleted
  case messageVideoProcessing
  case reactionCreated
  case giftOpened
  case momentUpdated
  case momentProcessing
  case userUpdated
  case userCounters
  case financeTransaction
  case chatAction
  case interactiveAction
  case userBlocked
  case systemMessage
  case customScreenUpdated
}

public enum SubscriptionRouting: Sendable {
  public static let presentationOnly: Set<SubscriptionName> = [
    .chatAction,
    .interactiveAction,
    .userBlocked,
    .systemMessage,
    .customScreenUpdated
  ]

  public static func writesToStore(_ name: SubscriptionName) -> Bool {
    !presentationOnly.contains(name)
  }

  public static func debounce(for name: SubscriptionName) -> TimeInterval? {
    switch name {
    case .chatUpdated:
      0.3
    case .userCounters:
      0.5
    default:
      nil
    }
  }

  public static func topic(userId: String) -> String {
    "graphql:user:\(userId)"
  }
}

public enum ReactionTarget: Sendable, Equatable, Decodable {
  case message(id: String, updatedAt: String, chatId: String)
  case moment(id: String, updatedAt: String)

  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let typeName = try container.decode(String.self, forKey: .typeName)

    switch typeName {
    case "Message":
      let id = try container.decode(String.self, forKey: .id)
      let updatedAt = try container.decode(String.self, forKey: .updatedAt)
      let chatId = try container.decode(String.self, forKey: .chatId)
      self = .message(id: id, updatedAt: updatedAt, chatId: chatId)
    case "Moment":
      let id = try container.decode(String.self, forKey: .id)
      let updatedAt = try container.decode(String.self, forKey: .updatedAt)
      self = .moment(id: id, updatedAt: updatedAt)
    default:
      throw DecodingError.dataCorrupted(
        DecodingError.Context(
          codingPath: container.codingPath,
          debugDescription: "Unsupported reaction target type: \(typeName)"
        )
      )
    }
  }

  private enum CodingKeys: String, CodingKey {
    case typeName = "__typename"
    case id
    case updatedAt
    case chatId
  }
}
