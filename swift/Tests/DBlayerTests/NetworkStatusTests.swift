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
    try DB.database.setOnline(online: false)
    let status = NetworkStatus()
    #expect(status.isOnline == false)
    let one = Cursor(status.changes)
    let two = Cursor(status.changes)
    #expect(await one.next("the current value of the first reader") == false)
    #expect(await two.next("the current value of the second reader") == false)
    try DB.database.setOnline(online: true)
    #expect(await one.next("the change for the first reader") == true)
    #expect(await two.next("the change for the second reader") == true)
    #expect(status.isOnline == true)
    try DB.database.setOnline(online: false)
    #expect(await one.next("the second change") == false)
    #expect(NetworkStatus().isOnline == false)
  }

  /// net_status_same_value_silent, net_status_ignores_server_and_session: the same value again and a server answer wake nobody, so the first frame after them is the next real change.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftAServerAnswerAndARepeatedValueGiveNoFrame() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    try DB.database.setOnline(online: true)
    let status = NetworkStatus()
    let reader = Cursor(status.changes)
    #expect(await reader.next("the current value") == true)
    try DB.database.setOnline(online: true)
    try DB.database.setOnline(online: true)
    _ = try await Moment.create(text: "answered", in: DB.account()).optimisticValue!
    #expect(status.isOnline == true)
    try DB.database.setOnline(online: false)
    #expect(await reader.next("the first frame after the repeats and the server work") == false)
    #expect(status.isOnline == false)
  }

  /// net_status_reader_gets_current: a reader that starts late gets the newest value first.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftALateReaderGetsTheNewestValueFirst() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    try DB.database.setOnline(online: true)
    try DB.database.setOnline(online: false)
    let late = Cursor(NetworkStatus().changes)
    #expect(await late.next("the first frame of a late reader") == false)
  }

  /// net_status_cancel_one_reader: cancelling the task of one reader ends that reader and the other one still gets the next value.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func net_status_swiftCancellingTheTaskEndsOnlyThatReader() async throws {
    try await openSignedOffline(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    try DB.database.setOnline(online: false)
    let status = NetworkStatus()
    let (frames, ready) = AsyncStream<Bool>.makeStream()
    let ending = Task { for await value in status.changes { ready.yield(value) } }
    let kept = Cursor(status.changes)
    #expect(await kept.next("the current value of the kept reader") == false)
    var seen = frames.makeAsyncIterator()
    #expect(await seen.next() == false)
    ending.cancel()
    await ending.value
    try DB.database.setOnline(online: true)
    #expect(await kept.next("the change for the kept reader") == true)
  }
}
