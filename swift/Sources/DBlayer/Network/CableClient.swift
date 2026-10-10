
import Foundation

public enum CableClientError: Error, Equatable, Sendable {
  case rejected
}

public actor CableClient {
  private let connection: CableConnection
  private let makeChannelId: @Sendable () -> String
  private var subscriptions: [String: CableSubscription] = [:]
  private var operationOrder: [String] = []
  private var sharedChannelId: String?
  private var isSharedRequested = false
  private var isSharedConfirmed = false
  private var hasWelcomed = false
  private let pingInterval: TimeInterval
  private var presenceUserId: String?
  private var presenceTask: Task<Void, Never>?
  private var isPingPaused = false
  private var isPresenceConfirmed = false
  private let timers: CableTimers

  public init(
    url: URL,
    token: String,
    makeSocket: @Sendable @escaping (URL, String) -> any CableSocket,
    makeChannelId: @Sendable @escaping () -> String = { UUID().uuidString },
    pingInterval: TimeInterval = OnlineChannel.pingInterval
  ) {
    self.init(
      url: url,
      token: token,
      makeSocket: makeSocket,
      makeChannelId: makeChannelId,
      pingInterval: pingInterval,
      timers: .live
    )
  }

  public init(
    url: URL,
    token: String,
    makeSocket: @Sendable @escaping (URL, String) -> any CableSocket,
    makeChannelId: @Sendable @escaping () -> String,
    pingInterval: TimeInterval,
    timers: CableTimers
  ) {
    connection = CableConnection(url: url, token: token, makeSocket: makeSocket, timers: timers)
    self.makeChannelId = makeChannelId
    self.pingInterval = pingInterval
    self.timers = timers
  }

  public func openPresence(userId: String) {
    presenceUserId = userId
    if hasWelcomed {
      Task { [weak self] in
        try? await self?.sendPresenceSubscribe()
      }
    }
  }

  public func start() async {
    let frames = await connection.frames
    await connection.open()
    for await frame in frames {
      await handle(frame)
    }
    await connection.close()
    finishSubscriptions()
  }

  public func subscribeGraphQL(
    document: String,
    operationName: String
  ) -> AsyncThrowingStream<GraphQLSubscriptionEvent, Error> {
    if sharedChannelId == nil {
      sharedChannelId = makeChannelId()
    }
    let operationId = makeChannelId()
    let stream = AsyncThrowingStream<GraphQLSubscriptionEvent, Error>.makeStream()
    subscriptions[operationId] = CableSubscription(
      operationId: operationId,
      document: document,
      operationName: operationName,
      continuation: stream.continuation
    )
    operationOrder.append(operationId)
    if hasWelcomed {
      Task { [weak self] in
        await self?.activate(operationId: operationId)
      }
    }
    return stream.stream
  }

  public func replaceToken(_ token: String) async {
    hasWelcomed = false
    isPresenceConfirmed = false
    resetSharedState()
    presenceTask?.cancel()
    presenceTask = nil
    sharedChannelId = subscriptions.isEmpty ? nil : makeChannelId()
    var rekeyed: [String: CableSubscription] = [:]
    var order: [String] = []
    for operationId in operationOrder {
      guard let subscription = subscriptions[operationId] else {
        continue
      }
      let newId = makeChannelId()
      rekeyed[newId] = subscription.rekeyed(operationId: newId)
      order.append(newId)
    }
    subscriptions = rekeyed
    operationOrder = order
    await connection.replaceToken(token, channels: channelCount)
  }

  public var isConnected: Bool {
    get async { await connection.state == .open }
  }

  public func foregroundCheck() async {
    await connection.foregroundCheck()
  }

  public func reconnect(cause: String) async {
    await connection.reset(cause: cause, channels: channelCount)
  }

  public func pausePing() {
    isPingPaused = true
    presenceTask?.cancel()
    presenceTask = nil
  }

  public func resumePing() async {
    isPingPaused = false
    await connection.resetAttempts()
    if isPresenceConfirmed, let presenceUserId {
      startPresencePing(userId: presenceUserId)
    }
  }

  public func stop() {
    let connection = connection
    Task {
      await connection.close()
    }
    finishSubscriptions()
  }

  private func handle(_ frame: CableFrame) async {
    switch frame {
    case .welcome:
      await resubscribeAll()
    case let .confirmed(identifier):
      if let presenceUserId, identifier == OnlineChannel.identifier(userId: presenceUserId) {
        isPresenceConfirmed = true
        if !isPingPaused {
          startPresencePing(userId: presenceUserId)
        }
        return
      }
      await handleConfirmation(identifier: identifier)
    case let .rejected(identifier):
      if identifier == sharedIdentifier {
        resetSharedState()
        finishSubscriptions(error: CableClientError.rejected)
      }
    case let .message(identifier, payload):
      handleMessage(identifier: identifier, payload: payload)
    case let .disconnect(_, reconnect):
      await closeConnection(reconnect: reconnect)
    case .closed:
      markSocketClosed()
    case .ping:
      break
    }
  }

  private func resubscribeAll() async {
    hasWelcomed = true
    resetSharedState()
    if let sharedIdentifier, !subscriptions.isEmpty {
      isSharedRequested = true
      try? await connection.send(.subscribe(identifier: sharedIdentifier))
    }
    try? await sendPresenceSubscribe()
  }

  private func closeConnection(reconnect: Bool) async {
    if reconnect {
      await connection.closeSocket()
    } else {
      await connection.close()
    }
  }

  private func markSocketClosed() {
    hasWelcomed = false
    isPresenceConfirmed = false
    resetSharedState()
    presenceTask?.cancel()
    presenceTask = nil
  }

  private func handleConfirmation(identifier: String) async {
    guard identifier == sharedIdentifier else {
      return
    }
    isSharedConfirmed = true
    for operationId in operationOrder {
      await execute(operationId: operationId)
    }
  }

  private func activate(operationId: String) async {
    if isSharedConfirmed {
      await execute(operationId: operationId)
      return
    }
    guard !isSharedRequested, let sharedIdentifier else {
      return
    }
    isSharedRequested = true
    try? await connection.send(.subscribe(identifier: sharedIdentifier))
  }

  private func execute(operationId: String) async {
    guard let sharedChannelId, var subscription = subscriptions[operationId] else {
      return
    }
    subscription.attempt += 1
    subscription.serverId = nil
    subscriptions[operationId] = subscription
    try? await connection.send(
      GraphQLChannel.execute(
        channelId: sharedChannelId,
        operationId: operationId,
        attempt: subscription.attempt,
        document: subscription.document,
        operationName: subscription.operationName
      )
    )
  }

  private func handleMessage(identifier: String, payload: JSONValue) {
    guard identifier == sharedIdentifier else {
      return
    }
    let route = GraphQLChannel.route(of: payload)
    guard let operationId = operationId(for: route), var subscription = subscriptions[operationId] else {
      return
    }
    if let subscriptionId = route.subscriptionId {
      subscription.serverId = subscriptionId
    }
    let requestId = subscription.requestId
    guard let event = GraphQLChannel.event(from: payload, requestId: requestId) else {
      subscriptions[operationId] = subscription
      return
    }
    switch event {
    case .ready:
      guard subscription.readyRequestIds.insert(requestId).inserted else {
        return
      }
      subscription.continuation.yield(.ready)
    case let .payload(value):
      subscription.continuation.yield(.payload(value))
    case let .errors(messages):
      subscription.continuation.yield(.errors(messages))
    case .completed:
      finishSubscription(operationId: operationId, error: nil)
      guard subscriptions.isEmpty, let sharedIdentifier else {
        return
      }
      resetSharedState()
      Task { [weak self] in
        try? await self?.connection.send(.unsubscribe(identifier: sharedIdentifier))
      }
      return
    }
    subscriptions[operationId] = subscription
  }

  private var sharedIdentifier: String? {
    sharedChannelId.map { GraphQLChannel.identifier(channelId: $0) }
  }

  private var channelCount: Int {
    (subscriptions.isEmpty ? 0 : 1) + (presenceUserId == nil ? 0 : 1)
  }

  private func operationId(for route: GraphQLChannel.Route) -> String? {
    if let requestId = route.requestId {
      return subscriptions.first { $0.value.requestId == requestId }?.key
    }
    guard let subscriptionId = route.subscriptionId else {
      return nil
    }
    return subscriptions.first { $0.value.serverId == subscriptionId }?.key
  }

  private func resetSharedState() {
    isSharedRequested = false
    isSharedConfirmed = false
  }

  private func sendPresenceSubscribe() async throws {
    guard let presenceUserId else {
      return
    }
    try await connection.send(OnlineChannel.subscribe(userId: presenceUserId))
  }

  private func startPresencePing(userId: String) {
    presenceTask?.cancel()
    let interval = pingInterval
    let timers = timers
    presenceTask = Task { [weak self] in
      while !Task.isCancelled {
        try? await self?.connection.send(OnlineChannel.ping(userId: userId, at: Date()))
        await timers.sleep(interval)
      }
    }
  }

  private func finishSubscription(operationId: String, error: (any Error)?) {
    operationOrder.removeAll { $0 == operationId }
    subscriptions.removeValue(forKey: operationId)?.continuation.finish(throwing: error)
  }

  private func finishSubscriptions(error: (any Error)? = nil) {
    presenceTask?.cancel()
    presenceTask = nil
    let activeSubscriptions = subscriptions.values
    subscriptions.removeAll()
    operationOrder.removeAll()
    for subscription in activeSubscriptions {
      subscription.continuation.finish(throwing: error)
    }
  }
}
