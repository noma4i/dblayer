import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private enum PresenceOnline: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "PresenceOnline"
  static let document = "query PresenceOnline { presenceOnline { id userId room } }"
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func openTestingKeepsScopePreloadsQuietUnlessAskedAndLoadsThemWhenAsked() async throws {
    for preload in [false, true] {
      DB.close()
      let transport = MockTransport()
      transport.enqueue("PresenceOnline", reply: #"{"data":{"presenceOnline":[]}}"#)
      try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, operations: [PresenceOnline.self], preload: preload)
      if preload {
        await eventually("the queued preload reply is consumed") { transport.calls.count == 1 }
      } else {
        #expect(transport.calls.isEmpty)
      }
      #expect(transport.unscripted.isEmpty)
    }
    DB.close()
  }
}
