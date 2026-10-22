import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

struct IdempotencyTests {
  @Test
  func aQueuedMutationKeepsItsKeyAcrossARefusalAndUsesAnExplicitKey() async throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let fixture = try JSONDecoder().decode(JSONValue.self, from: Data(contentsOf: root.appendingPathComponent("fixtures/idempotency.json"))).objectValue
    for explicit in [false, true] {
      let transport = MockTransport()
      transport.enqueue("MessageSend", reply: fixture["refusal"]!.text)
      transport.enqueue("MessageSend", reply: fixture["answer"]!.text)
      let database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: fixture["models"]!.text, transport: transport)
      try database.registerOperations(defsJson: fixture["operations"]!.text)
      let row = try await database.create(model: "Message", valuesJson: #"{"text":"hello"}"#, argsJson: explicit ? #"{"clientMutationId":"app-key"}"# : "{}")
      try await database.flush()
      _ = try await database.retry(model: "Message", rowId: row.id)
      let calls = transport.calls.filter { $0.operationName == "MessageSend" }
      #expect(calls.count == 2)
      let first = JSONValue.parse(calls[0].variablesJson).objectValue["input"]!.objectValue
      let key = try #require(first["clientMutationId"]?.stringValue)
      #expect(explicit ? key == "app-key" : key.count == 32)
      #expect(JSONValue.parse(calls[0].variablesJson) == JSONValue.parse(calls[1].variablesJson))
    }
  }
}
