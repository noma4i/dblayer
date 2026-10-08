import Foundation
import Testing
@testable import DBlayer

struct CableHeartbeatTests {
  @Test
  func stalenessUsesLastFrame() {
    let lastFrameAt = Date(timeIntervalSince1970: 0)

    #expect(CableHeartbeat.isStale(lastFrameAt: lastFrameAt, now: Date(timeIntervalSince1970: 5.9)) == false)
    #expect(CableHeartbeat.isStale(lastFrameAt: lastFrameAt, now: Date(timeIntervalSince1970: 6.1)) == true)
    #expect(CableHeartbeat.isStale(lastFrameAt: nil, now: Date(timeIntervalSince1970: 0)) == true)
  }

  @Test
  func pollIntervalGrowsCapsAndJitters() {
    #expect(CableHeartbeat.pollInterval(attempts: 0, jitter: 0) == 6)
    #expect(CableHeartbeat.pollInterval(attempts: 1, jitter: 0) == 6 * 1.15)
    #expect(
      abs(CableHeartbeat.pollInterval(attempts: 3, jitter: 0) - 6 * pow(1.15, 3)) < 0.0001
    )

    #expect(
      CableHeartbeat.pollInterval(attempts: 25, jitter: 0)
        == CableHeartbeat.pollInterval(attempts: 10, jitter: 0)
    )

    #expect(CableHeartbeat.pollInterval(attempts: 0, jitter: 0.5) == 9)

    #expect(CableHeartbeat.jitterBound(attempts: 0) == 1)
    #expect(CableHeartbeat.jitterBound(attempts: 1) == 0.15)
    #expect(CableHeartbeat.jitterBound(attempts: 7) == 0.15)
  }

  @Test
  func rejectsWrongSubprotocol() {
    #expect(URLSessionCableSocket.isSupported(protocol: "actioncable-v1-json") == true)
    #expect(URLSessionCableSocket.isSupported(protocol: "actioncable-unsupported") == false)
    #expect(URLSessionCableSocket.isSupported(protocol: nil) == false)
  }
}
