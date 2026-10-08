import Testing
@testable import DBlayerCore

struct BridgeProbeTests {
  @Test
  func versionComesFromRust() {
    #expect(version() == "0.1.0")
  }

  @Test
  func storedPayloadReadsBack() throws {
    let store = try Store.openInMemory()
    try store.put(model: "moments", id: "m1", payload: "{\"a\":1}")

    #expect(try store.get(model: "moments", id: "m1") == "{\"a\":1}")
    #expect(try store.get(model: "moments", id: "m2") == nil)
  }
}
