@testable import DBlayer
import Foundation
import Testing

@Suite
struct NestedTypesTests {
  private func withoutNullKeys(_ value: JSONValue) -> JSONValue {
    switch value {
    case let .object(fields):
      return .object(fields.filter { $0.value != .null }.mapValues(withoutNullKeys))
    case let .array(items):
      return .array(items.map(withoutNullKeys))
    default:
      return value
    }
  }

  @Test
  func generatedNestedTypesDecodeTheSharedCases() throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let cases = try JSONDecoder().decode([JSONValue].self, from: Data(contentsOf: root.appendingPathComponent("fixtures/nested.json")))
    for row in cases {
      let fields = row.objectValue
      let label = fields["name"]!.stringValue!
      let input = try fields["inputText"]?.stringValue.map { Data($0.utf8) } ?? JSONEncoder().encode(fields["input"]!)
      if fields["error"] == .bool(true) {
        #expect(throws: (any Error).self, Comment(rawValue: label)) {
          try JSONDecoder().decode(NestedValue.self, from: input)
        }
      } else {
        let decoded = try JSONDecoder().decode(NestedValue.self, from: input)
        let encoded = try JSONDecoder().decode(JSONValue.self, from: JSONEncoder().encode(decoded))
        #expect(withoutNullKeys(encoded) == withoutNullKeys(fields["output"]!), Comment(rawValue: label))
      }
    }
    let reply = try JSONDecoder().decode(RecordReplyNode.self, from: Data(#"{"userId":23,"user":{"id":"u1","fullName":"Ada"},"media":{}}"#.utf8))
    #expect(reply.userId == "23" && reply.user?.fullName == "Ada" && reply.media?.id == "")
    let tree = try JSONDecoder().decode(NestedTree.self, from: Data(#"{"name":"root","children":[{"name":"leaf"}]}"#.utf8))
    #expect(tree.children.first?.name == "leaf" && tree.children.first?.children == [])
  }
}

@Suite(.serialized)
struct CellDecoderTests {
  private func open() async throws -> Database {
    try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
  }

  @Test
  func aStoredRowDecodesFromCellsLikeTheSharedJSONCases() async throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let cases = try JSONDecoder().decode([JSONValue].self, from: Data(contentsOf: root.appendingPathComponent("fixtures/nested.json")))
    let database = try await open()
    for row in cases {
      let fields = row.objectValue
      guard fields["error"] != .bool(true), let input = fields["input"] else { continue }
      let label = fields["name"]!.stringValue!
      let values = String(decoding: try JSONEncoder().encode(["value": input]), as: UTF8.self)
      let wire = try await database.create(model: "NestedSample", valuesJson: values, argsJson: "{}")
      let sample = try NestedSample.decode(wire)
      let expected = try JSONDecoder().decode(NestedValue.self, from: JSONEncoder().encode(fields["output"]!))
      #expect(sample.value == expected, Comment(rawValue: label))
    }
  }

  @Test
  func aRowWithAnotherLayoutIsRefused() async throws {
    let database = try await open()
    var wire = try await database.create(model: "NestedSample", valuesJson: "{}", argsJson: "{}")
    wire.layout = "other"
    #expect(throws: (any Error).self) { try NestedSample.decode(wire) }
  }
}
