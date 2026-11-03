import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private enum PresenceOnline: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "PresenceOnline"
  static let document = "query PresenceOnline { presenceOnline { id userId room } }"
}

private enum PresenceHistory: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "PresenceHistory"
  static let document = "query PresenceHistory($presenceId: ID!) { presenceHistory(presenceId: $presenceId) { id userId room } }"
}

private enum PresenceOnChannel: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "PresenceOnChannel"
  static let document = "query PresenceOnChannel($channel: String!) { presenceOnChannel(channel: $channel) { id userId room } }"
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func openTestingKeepsScopePreloadsQuietUnlessAskedAndLoadsThemWhenAsked() async throws {
    for preload in [false, true] {
      try await DB.close()
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
    try await DB.close()
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aPreloadScopeDerivedFromAnotherScopeLoadsOnceForEachRowOfIt() async throws {
    try await DB.close()
    let transport = MockTransport()
    transport.enqueue("PresenceOnline", reply: #"{"data":{"presenceOnline":[{"id":"p1","userId":"u1","room":"a"},{"id":"p2","userId":"u2","room":"b"}]}}"#)
    transport.enqueue("PresenceHistory", reply: #"{"data":{"presenceHistory":[]}}"#)
    transport.enqueue("PresenceHistory", reply: #"{"data":{"presenceHistory":[]}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, operations: [PresenceOnline.self, PresenceHistory.self], preload: true)
    await eventually("one history load for each online row") { transport.calls.count == 3 }
    let loaded = transport.calls.filter { $0.operationName == "PresenceHistory" }.compactMap { call -> String? in
      let object = try? JSONSerialization.jsonObject(with: Data(call.variablesJson.utf8)) as? [String: Any]
      return object?["presenceId"] as? String
    }
    #expect(loaded.sorted() == ["p1", "p2"])
    #expect(transport.unscripted.isEmpty)
    try await DB.close()
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func refreshPreloadsReloadsEveryPreloadScopeEvenWhenItIsFresh() async throws {
    try await DB.close()
    let transport = MockTransport()
    transport.enqueue("PresenceOnline", reply: #"{"data":{"presenceOnline":[{"id":"p1","userId":"u1","room":"a"},{"id":"p2","userId":"u2","room":"b"}]}}"#)
    transport.enqueue("PresenceOnline", reply: #"{"data":{"presenceOnline":[{"id":"p1","userId":"u1","room":"a"},{"id":"p2","userId":"u2","room":"b"}]}}"#)
    for _ in 0..<4 {
      transport.enqueue("PresenceHistory", reply: #"{"data":{"presenceHistory":[]}}"#)
      transport.enqueue("PresenceOnChannel", reply: #"{"data":{"presenceOnChannel":[]}}"#)
    }
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, operations: [PresenceOnline.self, PresenceHistory.self, PresenceOnChannel.self], preload: true)
    await eventually("the preload loads every scope") { transport.calls.count == 5 }
    try await DB.refreshPreloads()
    #expect(transport.calls.count == 10)
    #expect(transport.unscripted.isEmpty)
    try await DB.close()
  }
}
