import Foundation
import Testing
@testable import DBlayer

struct CableIdleTests {
  @Test
  func aConnectionThatReceivesFramesWakesNoTimer() async {
    let bench = CableBench()
    let connection = bench.makeConnection()
    await connection.open()
    bench.sockets[0].deliver("{\"type\":\"welcome\"}")
    await eventually("the welcome frame") { await connection.lastFrameAt != nil }

    for step in 1 ... 10 {
      await bench.clock.advance(by: 3)
      bench.sockets[0].deliver("{\"type\":\"ping\",\"message\":\(step)}")
      await eventually("frame \(step)") { await connection.lastFrameAt == bench.clock.now() }
    }

    #expect(bench.clock.firedSleeps == 0)
    #expect(bench.sockets.count == 1)

    await bench.clock.advance(by: CableHeartbeat.staleAfter)
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.reopenDelay)
    await bench.clock.advance(by: CableHeartbeat.reopenDelay)
    await eventually("the reopen after silence") { bench.sockets.count == 2 }
    #expect(bench.sockets.count == 2)
    await connection.close()
  }

  @Test
  func failedOpensBackOffByTheFormulaAndAWelcomeResetsIt() async {
    let bench = CableBench()
    bench.failOpens = 3
    let connection = bench.makeConnection()
    await connection.open()

    var waits: [TimeInterval] = []
    for _ in 0 ..< 3 {
      await eventually("the next reconnect deadline") { bench.clock.pendingSeconds().contains { $0 != CableHeartbeat.reopenDelay } }
      let wait = bench.clock.pendingSeconds().first { $0 != CableHeartbeat.reopenDelay } ?? 0
      waits.append(wait)
      await bench.clock.advance(by: wait)
      await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.reopenDelay)
      await bench.clock.advance(by: CableHeartbeat.reopenDelay)
    }

    #expect(waits == (0 ..< 3).map { CableHeartbeat.pollInterval(attempts: $0, jitter: 0) })
    await eventually("the open that succeeds") { await connection.state == .open }
    bench.sockets.last?.deliver("{\"type\":\"welcome\"}")
    await eventually("the welcome frame") { await connection.lastFrameAt != nil }
    await eventually("the stale deadline after the welcome") { bench.clock.pendingSeconds() == [CableHeartbeat.staleAfter] }

    bench.sockets.last?.fail()
    await eventually("the closed socket") { await connection.state == .closed }
    await eventually("the reconnect deadline") { bench.clock.pendingSeconds().count == 1 }
    #expect(bench.clock.pendingSeconds() == [CableHeartbeat.pollInterval(attempts: 0, jitter: 0)])
    await connection.close()
  }

  @Test
  func aWallClockThatMovedBackDoesNotHideSilence() async {
    let bench = CableBench()
    let connection = bench.makeConnection()
    await connection.open()
    await eventually("the deadline of the open") { bench.clock.pendingDues() == [CableHeartbeat.staleAfter] }
    await bench.clock.advance(by: 1)
    bench.sockets[0].deliver("{\"type\":\"welcome\"}")
    await eventually("the welcome frame") { await connection.lastFrameAt != nil }
    let opened = CableHeartbeat.staleAfter
    #expect(!bench.clock.pendingDues().contains(opened))
    await eventually("the deadline of the frame") { bench.clock.pendingDues() == [1 + CableHeartbeat.staleAfter] }
    bench.clock.skewWallClock(by: -100)

    await bench.clock.advance(by: CableHeartbeat.staleAfter)
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.reopenDelay)
    await bench.clock.advance(by: CableHeartbeat.reopenDelay)
    await eventually("the reopen after silence") { bench.sockets.count == 2 }

    #expect(bench.clock.firedSleeps == 2)
    await eventually("the deadline of the new socket") { bench.clock.pendingSeconds() == [CableHeartbeat.staleAfter] }
    await connection.close()
  }
}
