import Testing
@testable import DBlayer

struct BridgeProbeTests {
  @Test
  func versionComesFromRust() {
    #expect(version() == "0.1.0")
  }
}
