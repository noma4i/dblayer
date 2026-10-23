import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private func storedScreenVector(_ index: Int) async throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let vectors = try JSONDecoder().decode([JSONValue].self, from: Data(contentsOf: root.appendingPathComponent("fixtures/screen.json")))
    let vector = vectors[index]
    var input = vector.objectValue
    guard case let .array(elements) = input["elements"], case let .array(state) = input["state"] else { Issue.record("missing input"); return }
    input["elements"] = .array(elements.map { element in
      var fields = element.objectValue
      for key in ["props", "visible", "press", "change"] {
        if let value = fields[key] { fields[key] = .string(value.text) }
      }
      return .object(fields)
    })
    input["state"] = .array(state.map { entry in
      var fields = entry.objectValue
      fields["value"] = .string(fields["value"]!.text)
      return .object(fields)
    })
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 0), modelsJson: modelsJSON, transport: transport)
    let document = "query ScreenNode($id: ID!) { node(id: $id) { ... on CustomScreen { id slug version requires { ios android } spec { root state { path value } elements { id type slot children padding margin background radius expand align accessibilityLabel props visible press change } } } } }"
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ScreenNode"), "document": .string(document)])]).text)
    let reply: JSONValue = .object(["data": .object(["node": .object(["__typename": .string("CustomScreen"), "id": .string("sample"), "slug": .string("sample"), "version": .integer(1), "requires": .object(["ios": .integer(2), "android": .null]), "spec": .object(input)])])])
    transport.enqueue("ScreenNode", reply: reply.text)
    _ = try await CustomScreen.node("sample")
    let saved = try #require(await CustomScreen.find("sample").current())
    #expect(saved.slug == "sample" && saved.version == 1 && saved.requires == ScreenRequires(ios: 2, android: nil))
    let stored = try #require(saved.spec)
    #expect(JSONValue.encode(stored.state) == vector.objectValue["state"])
    #expect(stored.elements.count == elements.count)
    for (element, source) in zip(stored.elements, elements) {
      let fields = source.objectValue
      #expect(element.type == fields["type"]?.stringValue)
      #expect(element.props == fields["props"])
      #expect(element.visible == fields["visible"])
      #expect(JSONValue.encode(element.press) == fields["press"])
      #expect(JSONValue.encode(element.change) == fields["change"])
    }
    transport.enqueue("ScreenNode", reply: #"{"data":{"node":{"__typename":"CustomScreen","id":"sample","slug":"updated"}}}"#)
    _ = try await CustomScreen.node("sample", force: true)
    #expect(try await CustomScreen.find("sample").current()?.spec == stored)
  }

  @Test(.timeLimit(.minutes(1)))
  func anUnknownElementTypeKeepsArbitraryPropsInStorage() async throws {
    try await storedScreenVector(0)
  }

  @Test(.timeLimit(.minutes(1)))
  func orderedActionListsKeepNestedConditionsInStorage() async throws {
    try await storedScreenVector(1)
  }
}
