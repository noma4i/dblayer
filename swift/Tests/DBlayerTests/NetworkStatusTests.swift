import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  /// net_status_first_value, net_status_change: the platform value is the public value and each reader gets a change once.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftReadersGetTheFirstValueAndEachChange() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    StatusProbe.shared.set(false)
    let status = StatusProbe.status()
    #expect(status.isOnline == false)
    let one = Cursor(status.changes)
    let two = Cursor(status.changes)
    #expect(await one.next("the current value of the first reader") == false)
    #expect(await two.next("the current value of the second reader") == false)
    StatusProbe.shared.set(true)
    #expect(await one.next("the change for the first reader") == true)
    #expect(await two.next("the change for the second reader") == true)
    #expect(status.isOnline == true)
    StatusProbe.shared.set(false)
    #expect(await one.next("the second change") == false)
    #expect(StatusProbe.status().isOnline == false)
  }

  /// net_status_same_value_silent, net_status_ignores_server_and_session: the same value again and a server answer wake nobody, so the first frame after them is the next real change.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftAServerAnswerAndARepeatedValueGiveNoFrame() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    StatusProbe.shared.set(true)
    let status = StatusProbe.status()
    let reader = Cursor(status.changes)
    #expect(await reader.next("the current value") == true)
    StatusProbe.shared.set(true)
    StatusProbe.shared.set(true)
    _ = try await Moment.create(text: "answered", in: DB.account()).optimisticValue!
    #expect(status.isOnline == true)
    StatusProbe.shared.set(false)
    #expect(await reader.next("the first frame after the repeats and the server work") == false)
    #expect(status.isOnline == false)
  }

  /// net_status_reader_gets_current: a reader that starts late gets the newest value first.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftALateReaderGetsTheNewestValueFirst() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    StatusProbe.shared.set(true)
    StatusProbe.shared.set(false)
    let late = Cursor(StatusProbe.status().changes)
    #expect(await late.next("the first frame of a late reader") == false)
  }

  /// net_status_cancel_one_reader: cancelling the task of one reader ends that reader and the other one still gets the next value.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftCancellingTheTaskEndsOnlyThatReader() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    StatusProbe.shared.set(false)
    let status = StatusProbe.status()
    let (frames, ready) = AsyncStream<Bool>.makeStream()
    let ending = Task {
      let reader = Cursor(status.changes)
      if let value = await reader.next("the current value of the ending reader") { ready.yield(value) }
      _ = await reader.next("the frame after the cancel")
    }
    let kept = Cursor(status.changes)
    #expect(await kept.next("the current value of the kept reader") == false)
    let seen = Cursor(frames)
    #expect(await seen.next("the frame the ending reader passed on") == false)
    ending.cancel()
    await ending.value
    StatusProbe.shared.set(true)
    #expect(await kept.next("the change for the kept reader") == true)
    StatusProbe.shared.set(false)
  }
}

/// The observer double of the status: the core starts the first one of the process and never stops it.
final class StatusProbe: PlatformObserver, @unchecked Sendable {
  static let shared = StatusProbe()
  private let lock = NSLock()
  private var sink: OnlineSink?
  private var started = 0

  static func status() -> NetworkStatus { NetworkStatus(observer: shared) }

  func start(sink: OnlineSink) { lock.withLock { self.sink = sink; started += 1 } }
  func stop() { Issue.record("the status observer was stopped") }

  var starts: Int { lock.withLock { started } }
  func set(_ online: Bool) { lock.withLock { sink }!.setOnline(online: online) }
}

extension BridgeProbeTests {
  /// net_status_follows_without_database, net_status_follows_after_close: the value arrives with no open database and after the database closed.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftFollowsWithoutADatabaseAndAfterClose() async throws {
    await DB.closeForCleanup()
    let status = StatusProbe.status()
    let reader = Cursor(status.changes)
    StatusProbe.shared.set(true)
    #expect(await reader.next("a value with no database") == true)
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    await DB.closeForCleanup()
    StatusProbe.shared.set(false)
    #expect(await reader.next("a value after the close") == false)
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
