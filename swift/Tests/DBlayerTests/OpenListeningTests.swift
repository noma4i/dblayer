import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private enum RecordEvents: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "RecordEvents"
  static let document = "subscription RecordEvents { recordEvents { entries { topic action recordId } } }"
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func inv_api_05_openTestingHoldsSubscriptionsUntilRegistrationCompletes() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, operations: [RecordEvents.self], listening: false)
    try await DB.resume()
    #expect(transport.calls.isEmpty)
    try DB.setListening(true)
    await eventually("registration complete starts the subscription") { transport.calls.count == 1 }
    try DB.setListening(true)
    try await DB.resume()
    #expect(transport.calls.map(\.operationName) == ["RecordEvents"])
  }
}
