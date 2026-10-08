import DBlayer
import Foundation
import Testing
@testable import DBlayer

extension CableLifecycleTests {
  enum Row: CaseIterable {
    case backgroundReportsOfflineKeepsSocket
    case foregroundRunsInParallel
    case foregroundResetsThenSyncs
    case networkRestoreReopensLostSocket
    case networkBlipKeepsChannelIds
    case newOfflineCancelsDeferredRestore
    case inactiveKeepsNetworkStateOnly
    case recycleLogsReasonAndChannelCount
    case resetsWithinHundredMsMerge
    case resetSkipsWhenSocketReopened
    case concurrentOpenersShareOneAttempt
    case resetRetryJoinsOtherCallerSocket
  }

  @Test(arguments: Row.allCases)
  func cableLifecycleRows(row: Row) async {
    let bench = CableBench(channelIds: ["old", "new"])
    switch row {
    case .backgroundReportsOfflineKeepsSocket:
      await backgroundReportsOfflineKeepsSocket(bench)
    case .foregroundRunsInParallel:
      await foregroundRunsInParallel(bench)
    case .foregroundResetsThenSyncs:
      await foregroundResetsThenSyncs(bench)
    case .networkRestoreReopensLostSocket:
      await networkRestoreEnsuresAliveThenSyncs(bench, socketLost: true)
    case .networkBlipKeepsChannelIds:
      await networkRestoreEnsuresAliveThenSyncs(bench, socketLost: false)
    case .newOfflineCancelsDeferredRestore:
      await newOfflineCancelsDeferredRestore(bench)
    case .inactiveKeepsNetworkStateOnly:
      await inactiveKeepsNetworkStateOnly(bench)
    default:
      await resetRow(row, bench)
    }
  }

  private func resetRow(_ row: Row, _ bench: CableBench) async {
    switch row {
    case .recycleLogsReasonAndChannelCount:
      await recycleLogsReasonAndChannelCount(bench)
    case .resetsWithinHundredMsMerge:
      await resetsWithinHundredMsMerge(bench)
    case .resetSkipsWhenSocketReopened:
      await resetSkipsWhenSocketReopened(bench)
    case .concurrentOpenersShareOneAttempt:
      await concurrentOpenersShareOneAttempt(bench)
    case .resetRetryJoinsOtherCallerSocket:
      await resetRetryJoinsOtherCallerSocket(bench)
    default:
      return
    }
  }

  private func makeLifecycle(client: CableClient, calls: CallLog) -> AppLifecycle {
    AppLifecycle(
      client: client,
      presence: { calls.add("presence \($0.rawValue)") },
      sync: { calls.add("sync \($0)") }
    )
  }

  private func makeRestore(client: CableClient, bench: CableBench, calls: CallLog) -> NetworkRestore {
    NetworkRestore(client: client, sync: { calls.add("sync \($0)") }, timers: bench.timers)
  }

  private func backgroundReportsOfflineKeepsSocket(_ bench: CableBench) async {
    let (client, start) = await bench.startWelcomedClient(presenceUserId: "54")
    await eventually { bench.clock.sleeperCount(requestedSeconds: 3600) == 1 }
    let calls = CallLog()
    let lifecycle = makeLifecycle(client: client, calls: calls)

    await lifecycle.phaseChanged(.inactive)
    await lifecycle.phaseChanged(.background)
    await lifecycle.phaseChanged(.background)
    await eventually { !calls.entries.isEmpty }
    await settle()

    #expect(calls.entries == ["presence offline"])
    #expect(!bench.sockets[0].isClosed)
    #expect(bench.clock.sleeperCount(requestedSeconds: 3600) == 0)
    await client.stop()
    await start.value
  }

  private func foregroundRunsInParallel(_ bench: CableBench) async {
    let (client, start) = await bench.startWelcomedClient(presenceUserId: "54")
    let calls = CallLog()
    let lifecycle = makeLifecycle(client: client, calls: calls)
    await lifecycle.phaseChanged(.background)
    await eventually { calls.entries == ["presence offline"] }

    let active = Task { await lifecycle.phaseChanged(.active) }
    await eventually { Set(calls.entries).isSuperset(of: ["presence online", "sync true"]) }
    await eventually { bench.pingCount == 2 }
    #expect(bench.clock.sleeperCount(requestedSeconds: 3600) == 1)
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.foregroundDelay)
    await bench.clock.advance(by: CableHeartbeat.foregroundDelay)
    await active.value

    #expect(Set(calls.entries) == ["presence offline", "presence online", "sync true"])
    #expect(bench.sockets.count == 1)
    await client.stop()
    await start.value
  }

  private func foregroundResetsThenSyncs(_ bench: CableBench) async {
    let (client, start) = await bench.startWelcomedClient(subscribing: "subscription X { x }")
    let calls = CallLog()
    let lifecycle = makeLifecycle(client: client, calls: calls)
    await lifecycle.phaseChanged(.background)
    bench.sockets[0].fail()
    await eventually { await !client.isConnected }

    let active = Task { await lifecycle.phaseChanged(.active) }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableReconnect.mergeDelay)
    #expect(!calls.entries.contains("sync true"))
    #expect(bench.sockets.count == 1)
    await bench.clock.advance(by: CableReconnect.mergeDelay)
    await active.value

    #expect(bench.sockets.count == 2)
    #expect(calls.entries.contains("sync true"))
    #expect(bench.recycles.map(\.cause) == ["app_foregrounded"])
    #expect(bench.recycles.map(\.drops) == [1])
    await client.stop()
    await start.value
  }

  private func recycleLogsReasonAndChannelCount(_ bench: CableBench) async {
    let (client, start) = await bench.startWelcomedClient(subscribing: "subscription X { x }")

    let reset = Task { await client.reconnect(cause: "manual") }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableReconnect.mergeDelay)
    await bench.clock.advance(by: CableReconnect.mergeDelay)
    await reset.value

    let recycle = bench.recycles.first
    #expect(bench.recycles.count == 1)
    #expect(recycle?.cause == "manual")
    #expect(recycle?.channels == 1)
    #expect(recycle?.drops == 0)
    #expect(abs((recycle?.uptime ?? 0) - CableReconnect.mergeDelay) < 0.001)
    await client.stop()
    await start.value
  }

  private func networkRestoreEnsuresAliveThenSyncs(_ bench: CableBench, socketLost: Bool) async {
    let (client, start) = await bench.startWelcomedClient(subscribing: "subscription X { x }")
    let calls = CallLog()
    let restore = makeRestore(client: client, bench: bench, calls: calls)
    if socketLost {
      bench.sockets[0].fail()
      await eventually { await !client.isConnected }
    }

    await restore.update(isOnline: false, subscriptionsActive: true)
    await restore.update(isOnline: true, subscriptionsActive: true)
    await bench.clock.waitForSleepers(1, requestedSeconds: 0.5)
    #expect(bench.sockets.count == 1)
    await bench.clock.advance(by: 0.5)
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.foregroundDelay)
    await bench.clock.waitForSleepers(1, requestedSeconds: 1)
    await bench.clock.advance(by: CableHeartbeat.foregroundDelay)
    if socketLost {
      await eventually { bench.sockets.count == 2 }
    }
    #expect(calls.entries.isEmpty)
    await bench.clock.advance(by: 0.8)
    await eventually { !calls.entries.isEmpty }

    #expect(bench.sockets.count == (socketLost ? 2 : 1))
    #expect(calls.entries == ["sync false"])
    #expect(bench.sockets.flatMap(\.sent).filter { $0.contains("new") }.isEmpty)
    await client.stop()
    await start.value
  }

  private func newOfflineCancelsDeferredRestore(_ bench: CableBench) async {
    let (client, start) = await bench.startWelcomedClient(subscribing: "subscription X { x }")
    let calls = CallLog()
    let restore = makeRestore(client: client, bench: bench, calls: calls)

    await restore.update(isOnline: false, subscriptionsActive: true)
    await restore.update(isOnline: true, subscriptionsActive: true)
    await bench.clock.waitForSleepers(1, requestedSeconds: 0.5)
    await restore.update(isOnline: false, subscriptionsActive: true)

    #expect(bench.clock.sleeperCount(requestedSeconds: 0.5) == 0)
    await client.stop()
    await start.value
  }

  private func inactiveKeepsNetworkStateOnly(_ bench: CableBench) async {
    let client = bench.makeClient()
    let calls = CallLog()
    let restore = makeRestore(client: client, bench: bench, calls: calls)

    await restore.update(isOnline: false, subscriptionsActive: true)
    await restore.update(isOnline: false, subscriptionsActive: false)
    #expect(await restore.isOnline == false)
    await restore.update(isOnline: true, subscriptionsActive: true)
    await settle()
    #expect(await restore.isOnline == true)

    #expect(bench.clock.sleeperCount(requestedSeconds: 0.5) == 0)
  }

  private func resetsWithinHundredMsMerge(_ bench: CableBench) async {
    let (client, start) = await bench.startWelcomedClient(subscribing: "subscription X { x }")

    let first = Task { await client.reconnect(cause: "first") }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableReconnect.mergeDelay)
    await bench.clock.advance(by: CableReconnect.mergeDelay / 2)
    let second = Task { await client.reconnect(cause: "second") }
    await eventually { bench.clock.sleeperDueTimes(before: 1).filter { $0 > CableReconnect.mergeDelay }.count == 1 }
    #expect(bench.clock.sleeperDueTimes(before: 1).count == 1)
    await bench.clock.advance(by: CableReconnect.mergeDelay)
    await first.value
    await second.value

    #expect(bench.log == ["open old-token", "close 0", "open old-token"])
    #expect(bench.recycles.map(\.cause) == ["second"])
    await client.stop()
    await start.value
  }

  private func resetSkipsWhenSocketReopened(_ bench: CableBench) async {
    let connection = bench.makeConnection()
    await connection.open()
    bench.sockets[0].fail()
    await eventually { await connection.state == .closed }

    let reset = Task { await connection.reset(cause: "app_foregrounded", channels: 0) }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableReconnect.mergeDelay)
    await bench.clock.advance(by: CableReconnect.mergeDelay / 2)
    await connection.open()
    await bench.clock.advance(by: CableReconnect.mergeDelay / 2)
    await reset.value

    #expect(bench.sockets.count == 2)
    #expect(bench.recycles.isEmpty)
    await connection.close()
  }

  private func concurrentOpenersShareOneAttempt(_ bench: CableBench) async {
    let connection = bench.makeConnection()
    await connection.open()
    bench.sockets[0].fail()
    await eventually { await connection.state == .closed }
    bench.gateOpens()

    let opening = Task { await connection.open() }
    await eventually { bench.gatedOpenCount == 1 }
    let check = Task { await connection.foregroundCheck() }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.foregroundDelay)
    await bench.clock.advance(by: CableHeartbeat.foregroundDelay)
    await settle()
    bench.releaseOpens()
    await opening.value
    await check.value

    #expect(bench.sockets.count == 2)
    await connection.close()
  }

  private func resetRetryJoinsOtherCallerSocket(_ bench: CableBench) async {
    let connection = bench.makeConnection()
    await connection.open()
    bench.sockets[0].fail()
    await eventually { await connection.state == .closed }
    bench.failOpens = 1

    let reset = Task { await connection.reset(cause: "app_foregrounded", channels: 0) }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableReconnect.mergeDelay)
    await bench.clock.advance(by: CableReconnect.mergeDelay)
    await bench.clock.waitForSleepers(1, requestedSeconds: 1)
    await connection.open()
    await bench.clock.advance(by: 1)
    await reset.value
    await settle()

    #expect(bench.sockets.count == 3)
    #expect(!bench.sockets[2].isClosed)
    await connection.close()
  }
}
