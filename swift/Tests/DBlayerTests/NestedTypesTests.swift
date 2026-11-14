import DBlayerTesting
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
      let fields = (try row.objectValue)
      guard fields["error"] != .bool(true), let output = fields["output"] else { continue }
      let label = fields["name"]!.stringValue!
      let decoded = try JSONDecoder().decode(NestedValue.self, from: JSONEncoder().encode(output))
      let encoded = try JSONDecoder().decode(JSONValue.self, from: JSONEncoder().encode(decoded))
      #expect(withoutNullKeys(encoded) == withoutNullKeys(output), Comment(rawValue: label))
    }
    let loose = Data(#"{"title":"loose","owner_id":42}"#.utf8)
    #expect(throws: (any Error).self) { try JSONDecoder().decode(NestedValue.self, from: loose) }
    let reply = try JSONDecoder().decode(RecordReplyNode.self, from: Data(#"{"userId":"23","user":{"id":"u1","fullName":"Ada"},"media":{"id":""}}"#.utf8))
    #expect(reply.userId == "23" && reply.user?.fullName == "Ada" && reply.media?.id == "")
    let tree = try JSONDecoder().decode(NestedTree.self, from: Data(#"{"name":"root","children":[{"name":"leaf","children":[]}]}"#.utf8))
    #expect(tree.children.first?.name == "leaf" && tree.children.first?.children == [])
  }
}

@Suite
struct CellDecoderTests {
  private func open() async throws -> Database {
    try await openDatabase(config: Config(path: nil), modelsJson: modelsJSON, transport: nil)
  }

  private func withoutUnknownKeys(_ value: JSONValue) -> JSONValue {
    switch value {
    case let .object(fields):
      return .object(fields.filter { $0.key != "unknown" }.mapValues(withoutUnknownKeys))
    case let .array(items):
      return .array(items.map(withoutUnknownKeys))
    default:
      return value
    }
  }

  @Test
  func aNestedKeyOutsideTheDeclaredTypeOrOfTheWrongTypeIsRefusedOnCreate() async throws {
    let database = try await open()
    for (values, path) in [
      (#"{"value":{"title":"t","child":{"id":"x"}}}"#, "value.child.id"),
      (#"{"value":{"title":"t","children":[{"body":"a"},{"nope":1}]}}"#, "value.children[1].nope"),
      (#"{"value":{"title":"t","count":"many"}}"#, "value.count"),
    ] {
      do {
        _ = try await database.create(epoch: database.sessionEpoch(), model: "NestedSample", valuesJson: values, argsJson: "{}")
        Issue.record(Comment(rawValue: "accepted \(values)"))
      } catch DbError.Invalid(_) {
      } catch {
        Issue.record(Comment(rawValue: "\(path): \(error)"))
      }
    }
    _ = try await database.create(epoch: database.sessionEpoch(), model: "NestedSample", valuesJson: #"{"value":{"title":"t","payload":{"anything":[1]}}}"#, argsJson: "{}")
  }

  @Test
  func aStoredRowDecodesFromCellsLikeTheSharedJSONCases() async throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let cases = try JSONDecoder().decode([JSONValue].self, from: Data(contentsOf: root.appendingPathComponent("fixtures/nested.json")))
    let database = try await open()
    for row in cases {
      let fields = (try row.objectValue)
      guard fields["error"] != .bool(true), let input = fields["input"] else { continue }
      let label = fields["name"]!.stringValue!
      let values = String(decoding: try JSONEncoder().encode(["value": withoutUnknownKeys(input)]), as: UTF8.self)
      let wire = try await database.create(epoch: database.sessionEpoch(), model: "NestedSample", valuesJson: values, argsJson: "{}").optimisticValue!
      let sample = try NestedSample.decode(wire)
      let expected = try JSONDecoder().decode(NestedValue.self, from: JSONEncoder().encode(fields["output"]!))
      #expect(sample.value == expected, Comment(rawValue: label))
    }
  }

  @Test
  func aRowWithAnotherLayoutIsRefused() async throws {
    let database = try await open()
    var wire = try await database.create(epoch: database.sessionEpoch(), model: "NestedSample", valuesJson: "{}", argsJson: "{}").optimisticValue!
    wire.layout = "other"
    #expect(throws: (any Error).self) { try NestedSample.decode(wire) }
  }
}
