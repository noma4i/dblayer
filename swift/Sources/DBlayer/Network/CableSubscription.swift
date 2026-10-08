import Foundation

public struct CableSubscription {
  public var channelId: String
  public var identifier: String
  public let document: String
  public let operationName: String
  public var attempt = 0
  public var readyRequestIds: Set<String> = []
  public let continuation: AsyncThrowingStream<GraphQLSubscriptionEvent, Error>.Continuation

  public func rekeyed(channelId: String) -> CableSubscription {
    var copy = self
    copy.channelId = channelId
    copy.identifier = GraphQLChannel.identifier(channelId: channelId)
    copy.attempt = 0
    copy.readyRequestIds = []
    return copy
  }
}
