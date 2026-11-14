import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  /// net_status_first_value, net_status_change: the platform value is the public value and each reader gets a change once.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftReadersGetTheFirstValueAndEachChange() async throws {
    try await openSignedOffline(config: Config(path: nil), modelsJson: modelsJSON)
    await StatusProbe.shared.set(false)
    let status = StatusProbe.status()
    #expect(status.isOnline == false)
    let one = Cursor(status.changes)
    let two = Cursor(status.changes)
    #expect(await one.next("the current value of the first reader")?.online == false)
    #expect(await two.next("the current value of the second reader")?.online == false)
    await StatusProbe.shared.set(true)
    #expect(await one.next("the change for the first reader")?.online == true)
    #expect(await two.next("the change for the second reader")?.online == true)
    #expect(status.isOnline == true)
    await StatusProbe.shared.set(false)
    #expect(await one.next("the second change")?.online == false)
    #expect(StatusProbe.status().isOnline == false)
  }

  /// net_status_reader_gets_current: a reader that starts late gets the newest value first.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftALateReaderGetsTheNewestValueFirst() async throws {
    try await openSignedOffline(config: Config(path: nil), modelsJson: modelsJSON)
    await StatusProbe.shared.set(true)
    await StatusProbe.shared.set(false)
    let late = Cursor(StatusProbe.status().changes)
    #expect(await late.next("the first frame of a late reader")?.online == false)
  }

  /// net_status_cancel_one_reader: cancelling the task of one reader ends that reader and the other one still gets the next value.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftCancellingTheTaskEndsOnlyThatReader() async throws {
    try await openSignedOffline(config: Config(path: nil), modelsJson: modelsJSON)
    await StatusProbe.shared.set(false)
    let status = StatusProbe.status()
    let (frames, ready) = AsyncStream<NetworkPath>.makeStream()
    let ending = Task {
      let reader = Cursor(status.changes)
      if let value = await reader.next("the current value of the ending reader") { ready.yield(value) }
      _ = await reader.next("the frame after the cancel")
    }
    let kept = Cursor(status.changes)
    #expect(await kept.next("the current value of the kept reader")?.online == false)
    let seen = Cursor(frames)
    #expect(await seen.next("the frame the ending reader passed on")?.online == false)
    ending.cancel()
    await ending.value
    await StatusProbe.shared.set(true)
    #expect(await kept.next("the change for the kept reader")?.online == true)
    await StatusProbe.shared.set(false)
  }
}

/// The observer double of the status: the core starts the first one of the process and never stops it.
final class StatusProbe: PlatformObserver, @unchecked Sendable {
  static let shared = StatusProbe()
  private let lock = NSLock()
  private var sink: OnlineSink?
  private var started = 0

  static func status() -> NetworkStatus { NetworkStatus(observer: shared) }

  func start(sink: OnlineSink) {
    lock.withLock { self.sink = sink; started += 1 }
    TestPulse.shared.pulse()
  }
  func stop() { Issue.record("the status observer was stopped") }

  var starts: Int { lock.withLock { started } }
  var isStarted: Bool { lock.withLock { sink != nil } }

  /// The core starts the observer on its own threads after `NetworkStatus(observer:)` returned: the first value waits for that start.
  private func started() async -> OnlineSink {
    await eventually("the status observer started") { isStarted }
    return lock.withLock { sink }!
  }

  func set(_ online: Bool) async { await started().setOnline(online: online) }
  func set(path: NetworkPath) async { await started().setPath(path: path) }
  /// The same value for a `defer`, after a `set` that waited for the start.
  func reset(_ online: Bool) { lock.withLock { sink }?.setOnline(online: online) }
}

extension BridgeProbeTests {
  /// bridge_network_path: the path the platform gives reaches `path`, `isOnline` and `changes` as the same values, and a change of the network while online is the next element.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func bridge_network_path_swiftCarriesThePathAndTheChangeOfNetwork() async throws {
    await DB.closeForCleanup()
    let status = StatusProbe.status()
    let reader = Cursor(status.changes)
    let wifi = NetworkPath(online: true, kind: .wifi, network: 11)
    let cellular = NetworkPath(online: true, kind: .cellular, network: 12)
    await StatusProbe.shared.set(path: wifi)
    #expect(await reader.next("the first path") == wifi)
    #expect(status.path == wifi)
    await StatusProbe.shared.set(path: cellular)
    #expect(await reader.next("the path after the change of network") == cellular)
    #expect(status.path == cellular && status.isOnline == true)
  }
}

extension BridgeProbeTests {
  /// net_status_follows_without_database, net_status_follows_after_close: the value arrives with no open database and after the database closed.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftFollowsWithoutADatabaseAndAfterClose() async throws {
    await DB.closeForCleanup()
    let status = StatusProbe.status()
    let reader = Cursor(status.changes)
    await StatusProbe.shared.set(true)
    #expect(await reader.next("a value with no database")?.online == true)
    try await openSignedOffline(config: Config(path: nil), modelsJson: modelsJSON)
    await DB.closeForCleanup()
    await StatusProbe.shared.set(false)
    #expect(await reader.next("a value after the close")?.online == false)
    #expect(status.isOnline == false)
  }

  /// net_status_observer_starts_once: a further `NetworkStatus` starts no second observer.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftFurtherStatusesStartNoSecondObserver() {
    _ = StatusProbe.status()
    let other = StatusProbe()
    _ = NetworkStatus(observer: other)
    _ = StatusProbe.status()
    #expect(StatusProbe.shared.starts == 1)
    #expect(other.starts == 0)
  }
}
