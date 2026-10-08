import Foundation
import Testing
@testable import DBlayer

struct CableReconnectTests {
  @Test
  func delayDoublesPerAttemptCapsAndAddsJitter() {
    #expect(CableReconnect.delay(attempt: 1, jitter: 0) == 0)
    #expect(CableReconnect.delay(attempt: 1, jitter: 1) == 0)
    #expect(abs(CableReconnect.delay(attempt: 2, jitter: 0) - 1) < 0.0001)
    #expect(abs(CableReconnect.delay(attempt: 3, jitter: 0) - 2) < 0.0001)
    #expect(abs(CableReconnect.delay(attempt: 4, jitter: 0) - 4) < 0.0001)
    #expect(abs(CableReconnect.delay(attempt: 20, jitter: 0) - 10) < 0.0001)
    #expect(abs(CableReconnect.delay(attempt: 30, jitter: 0) - 10) < 0.0001)
    #expect(abs(CableReconnect.delay(attempt: 2, jitter: 1) - 1.3) < 0.0001)
    #expect(abs(CableReconnect.delay(attempt: 2, jitter: 0.5) - 1.15) < 0.0001)
  }
}
