import DBlayer
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
    let reply = try JSONDecoder().decode(MessageReplyNode.self, from: Data(#"{"userId":23,"user":{"id":"u1","fullName":"Ada"},"media":{}}"#.utf8))
    #expect(reply.userId == "23" && reply.user?.fullName == "Ada" && reply.media?.id == "")
    let tree = try JSONDecoder().decode(NestedTree.self, from: Data(#"{"name":"root","children":[{"name":"leaf"}]}"#.utf8))
    #expect(tree.children.first?.name == "leaf" && tree.children.first?.children == [])
  }
}
