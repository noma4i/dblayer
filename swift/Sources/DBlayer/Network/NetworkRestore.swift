import Foundation

public actor NetworkRestore {
  private let client: CableClient
  private let sync: @Sendable (_ isForced: Bool) async -> Void
  private let timers: CableTimers
  private var wasOffline = false
  private var deferred: Task<Void, Never>?

  public private(set) var isOnline = true

  public init(client: CableClient, sync: @escaping @Sendable (_ isForced: Bool) async -> Void) {
    self.init(client: client, sync: sync, timers: .live)
  }

  public init(client: CableClient, sync: @escaping @Sendable (_ isForced: Bool) async -> Void, timers: CableTimers) {
    self.client = client
    self.sync = sync
    self.timers = timers
  }

  public func update(isOnline: Bool, subscriptionsActive: Bool) {
    self.isOnline = isOnline
    deferred?.cancel()
    deferred = nil
    guard subscriptionsActive else {
      wasOffline = false
      return
    }
    guard isOnline else {
      wasOffline = true
      return
    }
    guard wasOffline else { return }
    wasOffline = false
    let client = client
    let sync = sync
    let timers = timers
    deferred = Task {
      await timers.sleep(Self.aliveDelay)
      guard !Task.isCancelled else { return }
      async let alive: Void = client.foregroundCheck()
      await timers.sleep(Self.syncDelay)
      guard !Task.isCancelled else { return }
      await sync(false)
      await alive
    }
  }

  private static let aliveDelay: TimeInterval = 0.5
  private static let syncDelay: TimeInterval = 1
}
