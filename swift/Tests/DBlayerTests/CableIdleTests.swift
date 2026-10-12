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
}
