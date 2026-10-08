import DBlayer
import Foundation

public actor AppLifecycle {
  public enum Phase: Sendable {
    case active
    case inactive
    case background
  }

  private let client: CableClient
  private let presence: @Sendable (OnlineChannel.PresenceState) async -> Void
  private let sync: @Sendable (_ isForced: Bool) async -> Void
  private var isBackground = false

  public init(
    client: CableClient,
    presence: @escaping @Sendable (OnlineChannel.PresenceState) async -> Void,
    sync: @escaping @Sendable (_ isForced: Bool) async -> Void
  ) {
    self.client = client
    self.presence = presence
    self.sync = sync
  }

  public func phaseChanged(_ phase: Phase) async {
    switch phase {
    case .inactive:
      return
    case .background:
      guard !isBackground else { return }
      isBackground = true
      await client.pausePing()
      let presence = presence
      Task { await presence(.offline) }
    case .active:
      guard isBackground else { return }
      isBackground = false
      await resume()
    }
  }

  private func resume() async {
    await client.resumePing()
    async let online: Void = presence(.online)
    async let revived: Void = reviveSocketThenSync()
    _ = await (online, revived)
  }

  private func reviveSocketThenSync() async {
    guard await client.isConnected else {
      await client.reconnect(cause: "app_foregrounded")
      await sync(true)
      return
    }
    async let alive: Void = client.foregroundCheck()
    await sync(true)
    await alive
  }
}
