
import Foundation

public enum CableClientError: Error, Equatable, Sendable {
  case rejected
}

public actor CableClient {
  private let connection: CableConnection
  private let makeChannelId: @Sendable () -> String
  private var subscriptions: [String: CableSubscription] = [:]
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
    let channelId = makeChannelId()
    let identifier = GraphQLChannel.identifier(channelId: channelId)
    let stream = AsyncThrowingStream<GraphQLSubscriptionEvent, Error>.makeStream()
    subscriptions[channelId] = CableSubscription(
      channelId: channelId,
      identifier: identifier,
      document: document,
      operationName: operationName,
      continuation: stream.continuation
    )
    if hasWelcomed {
      Task { [weak self] in
        try? await self?.sendSubscribe(channelId: channelId)
      }
    }
    return stream.stream
  }

  public func replaceToken(_ token: String) async {
    hasWelcomed = false
    isPresenceConfirmed = false
    presenceTask?.cancel()
    presenceTask = nil
    let rekeyed = subscriptions.values.map { $0.rekeyed(channelId: makeChannelId()) }
    subscriptions = Dictionary(uniqueKeysWithValues: rekeyed.map { ($0.channelId, $0) })
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
      if let channelId = channelId(for: identifier) {
        finishSubscription(channelId: channelId, error: CableClientError.rejected)
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
    for channelId in Array(subscriptions.keys) {
      try? await sendSubscribe(channelId: channelId)
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
    presenceTask?.cancel()
    presenceTask = nil
  }

  private func handleConfirmation(identifier: String) async {
    guard let channelId = channelId(for: identifier), var subscription = subscriptions[channelId] else {
      return
    }
    subscription.attempt += 1
    subscriptions[channelId] = subscription
    try? await connection.send(
      GraphQLChannel.execute(
        channelId: subscription.channelId,
        attempt: subscription.attempt,
        document: subscription.document,
        operationName: subscription.operationName
      )
    )
  }

  private func handleMessage(identifier: String, payload: JSONValue) {
    guard let channelId = channelId(for: identifier), var subscription = subscriptions[channelId] else {
      return
    }
    let requestId = GraphQLChannel.requestId(
      channelId: subscription.channelId,
      attempt: subscription.attempt
    )
    guard let event = GraphQLChannel.event(from: payload, requestId: requestId) else {
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
      finishSubscription(channelId: channelId, error: nil)
      Task { [weak self] in
        try? await self?.connection.send(.unsubscribe(identifier: subscription.identifier))
      }
      return
    }
    subscriptions[channelId] = subscription
  }

  private func sendSubscribe(channelId: String) async throws {
    guard let subscription = subscriptions[channelId] else {
      return
    }
    try await connection.send(.subscribe(identifier: subscription.identifier))
  }

  private var channelCount: Int {
    subscriptions.count + (presenceUserId == nil ? 0 : 1)
  }

  private func channelId(for identifier: String) -> String? {
    subscriptions.first { $0.value.identifier == identifier }?.key
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

  private func finishSubscription(channelId: String, error: (any Error)?) {
    subscriptions.removeValue(forKey: channelId)?.continuation.finish(throwing: error)
  }

  private func finishSubscriptions() {
    presenceTask?.cancel()
    presenceTask = nil
    let activeSubscriptions = subscriptions.values
    subscriptions.removeAll()
    for subscription in activeSubscriptions {
      subscription.continuation.finish()
    }
  }
}
