import Foundation

public struct CableSubscription {
  public var operationId: String
  public let document: String
  public let operationName: String
  public var attempt = 0
  public var serverId: String?
  public var readyRequestIds: Set<String> = []
  public let continuation: AsyncThrowingStream<GraphQLSubscriptionEvent, Error>.Continuation

  public var requestId: String {
    GraphQLChannel.requestId(operationId: operationId, attempt: attempt)
  }

  public func rekeyed(operationId: String) -> CableSubscription {
    var copy = self
    copy.operationId = operationId
    copy.attempt = 0
    copy.serverId = nil
    copy.readyRequestIds = []
    return copy
  }
}
