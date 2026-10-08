import Foundation

public enum CableConnectionState: Sendable, Equatable {
  case idle, connecting, open, closing, closed
}

public actor CableConnection {
  private let url: URL
  private let makeSocket: @Sendable (URL, String) -> any CableSocket
  private let timers: CableTimers
  private let continuation: AsyncStream<CableFrame>.Continuation
  private let stream: AsyncStream<CableFrame>
  private var token: String
  private var socket: (any CableSocket)?
  private var generation = 0
  private var readTask: Task<Void, Never>?
  private var watchdogTask: Task<Void, Never>?
  private var resetTask: Task<Void, Never>?
  private var attempts = 0
  private var drops = 0
  private var pendingCause = ""
  private var pendingChannels = 0
  private var requestedAt = Date.distantPast
  private var attemptStartedAt = Date.distantPast
  private var openedAt: Date?
  private var closedAt: Date?
  private var isStopped = false

  private(set) var state = CableConnectionState.idle
  private(set) var lastFrameAt: Date?

  public init(
    url: URL,
    token: String,
    makeSocket: @Sendable @escaping (URL, String) -> any CableSocket,
    timers: CableTimers = .live
  ) {
    let stream = AsyncStream<CableFrame>.makeStream()
    self.url = url
    self.token = token
    self.makeSocket = makeSocket
    self.timers = timers
    self.stream = stream.stream
    continuation = stream.continuation
  }

  public var frames: AsyncStream<CableFrame> {
    stream
  }

  public func open() async {
    isStopped = false
    if watchdogTask == nil {
      watchdogTask = Task { await watch() }
    }
    _ = await openSocket()
  }

  public func closeSocket() {
    guard socket != nil || state == .connecting else {
      return
    }
    generation += 1
    state = .closing
    readTask?.cancel()
    readTask = nil
    socket?.close()
    socket = nil
    markClosed()
  }

  public func close() {
    isStopped = true
    watchdogTask?.cancel()
    watchdogTask = nil
    resetTask?.cancel()
    resetTask = nil
    generation += 1
    readTask?.cancel()
    readTask = nil
    socket?.close()
    socket = nil
    state = .closed
    continuation.finish()
  }

  public func send(_ command: CableCommand) async throws {
    guard let socket else {
      throw CableSocketError.closed
    }
    try await socket.send(command.json)
  }

  public func foregroundCheck() async {
    await timers.sleep(CableHeartbeat.foregroundDelay)
    guard !isStopped, state != .connecting, socket == nil || isStale() else {
      return
    }
    closeSocket()
    _ = await openSocket()
  }

  public func resetAttempts() {
    attempts = 0
  }

  public func replaceToken(_ newToken: String, channels: Int = 0) async {
    token = newToken
    await reset(cause: "token_replaced", channels: channels)
  }

  public func reset(cause: String, channels: Int) async {
    pendingCause = cause
    pendingChannels = channels
    requestedAt = timers.now()
    resetTask?.cancel()
    let task = Task { await runReset() }
    resetTask = task
    await task.value
  }

  private func runReset() async {
    await timers.sleep(CableReconnect.mergeDelay)
    var attempt = 0
    while !Task.isCancelled, !isStopped, !hasAttemptSinceRequest {
      attempt += 1
      if attempt == 1 {
        timers.recycled(CableRecycle(
          cause: pendingCause,
          uptime: openedAt.map { timers.now().timeIntervalSince($0) } ?? 0,
          channels: pendingChannels,
          drops: drops
        ))
      }
      let delay = CableReconnect.delay(attempt: attempt, jitter: timers.jitter(1))
      if delay > 0 {
        await timers.sleep(delay)
      }
      guard !Task.isCancelled, !hasAttemptSinceRequest else {
        return
      }
      closeSocket()
      if await openSocket() {
        return
      }
    }
  }

  private var hasAttemptSinceRequest: Bool {
    (socket != nil || state == .connecting) && attemptStartedAt > requestedAt
  }

  private func openSocket() async -> Bool {
    guard socket == nil, state != .connecting, !isStopped else {
      return socket != nil
    }
    attemptStartedAt = timers.now()
    state = .connecting
    generation += 1
    let current = generation
    let candidate = makeSocket(url, token)
    do {
      try await candidate.open()
    } catch {
      guard current == generation else {
        return false
      }
      if case CableSocketError.unsupportedFrame = error {
        close()
      } else {
        markClosed()
      }
      return false
    }
    guard current == generation, !isStopped else {
      candidate.close()
      return false
    }
    socket = candidate
    state = .open
    openedAt = timers.now()
    readTask = Task { await readFrames(from: candidate, generation: current) }
    return true
  }

  private func readFrames(from socket: any CableSocket, generation current: Int) async {
    do {
      while !Task.isCancelled {
        let text = try await socket.receive()
        guard current == generation, let frame = CableFrame(json: text) else {
          continue
        }
        lastFrameAt = timers.now()
        if frame == .welcome {
          attempts = 0
        }
        continuation.yield(frame)
      }
    } catch {
      guard current == generation else {
        return
      }
      socket.close()
      self.socket = nil
      readTask = nil
      drops += 1
      markClosed()
    }
  }

  private func markClosed() {
    state = .closed
    closedAt = timers.now()
    continuation.yield(.closed)
  }

  private func watch() async {
    while !Task.isCancelled, !isStopped {
      let bound = CableHeartbeat.jitterBound(attempts: attempts)
      await timers.sleep(CableHeartbeat.pollInterval(attempts: attempts, jitter: timers.jitter(bound)))
      guard !Task.isCancelled, !isStopped else {
        return
      }
      await reopenIfStale()
    }
  }

  private func reopenIfStale() async {
    guard state != .connecting, isStale() else {
      return
    }
    if let closedAt, timers.now().timeIntervalSince(closedAt) < CableHeartbeat.staleAfter {
      attempts += 1
      return
    }
    drops += 1
    closeSocket()
    await timers.sleep(CableHeartbeat.reopenDelay)
    guard !isStopped else {
      return
    }
    _ = await openSocket()
  }

  private func isStale() -> Bool {
    let latest = [lastFrameAt, openedAt].compactMap(\.self).max()
    return CableHeartbeat.isStale(lastFrameAt: latest, now: timers.now())
  }
}
