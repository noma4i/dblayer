import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let screenDocument = """
  query SampleNode($id: ID!) { node(id: $id) { ... on InterfaceRecord { id slug spec { root \
  elements { __typename id children press { __typename ... on SampleNavigateAction { slug title } ... on SampleOpenRecordAction { recordId } ... on SampleOpenExternalAction { url } ... on SampleOpenProfileAction { userId } } \
  visible { __typename ... on SampleCondition { comparison negated operand { __typename ... on SampleTextLiteral { text } ... on SampleStateRef { path } } path } ... on SampleVisibleAll { conditions { comparison negated path } } ... on SampleVisibleFlag { flag } } \
  ... on SampleButton { label { __typename ... on SampleStateRef { path } ... on SampleTextLiteral { text } } disabled { __typename ... on SampleFlagLiteral { flag } } } \
  ... on SampleColumn { gap { __typename ... on SampleNumberLiteral { number } } alignItems } ... on SampleHeading { text { __typename ... on SampleTextLiteral { text } } level } \
  ... on SampleSpacer { length { __typename ... on SampleNumberLiteral { number } } } ... on SampleSwitch { checked { __typename ... on SampleStateRef { path } } label { __typename ... on SampleTextLiteral { text } } } } \
  state { path value { __typename ... on SampleTextLiteral { text } ... on SampleNumberLiteral { number } } } } } } }
  """

  private static let screenElements = #"""
  [
    {"__typename":"SampleColumn","id":"root","children":["b1","h1","p1","sw1","sp1","f1"],"press":null,"visible":{"__typename":"SampleVisibleAll","conditions":[{"comparison":"EQ","negated":false,"operand":{"__typename":"SampleTextLiteral","text":"x"},"path":"user.name"}]},"gap":{"__typename":"SampleNumberLiteral","number":8},"alignItems":"center"},
    {"__typename":"SampleButton","id":"b1","children":[],"press":{"__typename":"SampleNavigateAction","slug":"home","title":"Home"},"visible":null,"label":{"__typename":"SampleStateRef","path":"user.name"},"disabled":{"__typename":"SampleFlagLiteral","flag":true}},
    {"__typename":"SampleHeading","id":"h1","children":[],"press":null,"visible":{"__typename":"SampleVisibleFlag","flag":true},"text":{"__typename":"SampleTextLiteral","text":"Hi"},"level":2},
    {"__typename":"SamplePressable","id":"p1","children":["h1"],"press":{"__typename":"SampleOpenRecordAction","recordId":"c9"},"visible":{"__typename":"SampleCondition","comparison":"NE","negated":true,"operand":{"__typename":"SampleStateRef","path":"flag"},"path":"a.b"}},
    {"__typename":"SampleSwitch","id":"sw1","children":[],"press":null,"visible":null,"checked":{"__typename":"SampleStateRef","path":"on"},"label":null},
    {"__typename":"SampleSpacer","id":"sp1","children":[],"press":null,"visible":null,"length":{"__typename":"SampleNumberLiteral","number":4.5}},
    {"__typename":"SampleFuture","id":"f1","children":["x"],"size":3}
  ]
  """#

  private static func screenReply(elements: String, state: String = #"[{"path":"user.name","value":{"__typename":"SampleTextLiteral","text":"Ada"}},{"path":"n","value":{"__typename":"SampleNumberLiteral","number":3}}]"#) -> String {
    #"{"data":{"node":{"__typename":"InterfaceRecord","id":"s1","slug":"main","spec":{"root":"root","elements":\#(elements),"state":\#(state)}}}}"#
  }

  private func openSampleRig() async throws -> MockTransport {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("SampleNode"), "document": .string(Self.screenDocument)])]).text)
    return transport
  }

  @Test(.timeLimit(.minutes(1)))
  func aServerAnswerCarriesEveryElementVariantAndALaterAnswerReplacesTheList() async throws {
    let transport = try await openSampleRig()
    transport.enqueue("SampleNode", reply: Self.screenReply(elements: Self.screenElements))
    let screen = try #require(try await InterfaceRecord.node("s1"))
    let spec = try #require(screen.spec)
    #expect(spec.root == "root" && spec.elements.count == 7)
    #expect(spec.elements.map(\.id) == ["root", "b1", "h1", "p1", "sw1", "sp1", nil])
    #expect(spec.elements.map(\.kind) == [.column, .button, .heading, .pressable, .switch, .spacer, nil])
    #expect(spec.elements[0].children == ["b1", "h1", "p1", "sw1", "sp1", "f1"])
    #expect(spec.elements[3].children == ["h1"])
    #expect(spec.elements[6] == .unknown(typename: "SampleFuture"))
    guard case let .button(button) = spec.elements[1] else { Issue.record("not a button"); return }
    #expect(button.press == .navigate(SampleNavigateAction(slug: "home", title: "Home")))
    #expect(button.label == .stateRef(SampleStateRef(path: "user.name")) && button.disabled == .flagLiteral(SampleFlagLiteral(flag: true)))
    guard case let .column(column) = spec.elements[0] else { Issue.record("not a column"); return }
    #expect(column.gap == .numberLiteral(SampleNumberLiteral(number: 8)) && column.alignItems == "center")
    #expect(column.visible == .visibleAll(SampleVisibleAll(conditions: [SampleCondition(comparison: "EQ", negated: false, operand: .textLiteral(SampleTextLiteral(text: "x")), path: "user.name")])))
    #expect(spec.elements[1].press == .navigate(SampleNavigateAction(slug: "home", title: "Home")))
    #expect(spec.elements[3].press == .openRecord(SampleOpenRecordAction(recordId: "c9")))
    #expect(spec.elements[3].visible == .condition(SampleCondition(comparison: "NE", negated: true, operand: .stateRef(SampleStateRef(path: "flag")), path: "a.b")))
    #expect(spec.elements[2].visible == .visibleFlag(SampleVisibleFlag(flag: true)))
    #expect(spec.state == [SampleStateEntry(path: "user.name", value: .text(SampleTextLiteral(text: "Ada"))), SampleStateEntry(path: "n", value: .number(SampleNumberLiteral(number: 3)))])

    let second = #"""
    [
      {"__typename":"SampleHeading","id":"b1","children":["q"],"press":{"__typename":"SampleOpenExternalAction","url":"https://x"},"visible":null,"text":{"__typename":"SampleTextLiteral","text":"now a heading"},"level":null},
      {"__typename":"SampleColumn","id":"root","children":["b1"],"press":null,"visible":null,"gap":null,"alignItems":null}
    ]
    """#
    transport.enqueue("SampleNode", reply: Self.screenReply(elements: second, state: "[]"))
    _ = try await InterfaceRecord.node("s1", force: true)
    let again = try #require(await InterfaceRecord.find("s1").current()?.spec)
    #expect(again.elements.map(\.id) == ["b1", "root"])
    #expect(again.elements.map(\.kind) == [.heading, .column])
    #expect(again.elements[0].children == ["q"] && again.elements[0].press == .openExternal(SampleOpenExternalAction(url: "https://x")))
    #expect(again.elements[1].visible == nil && again.state.isEmpty)
    guard case let .heading(heading) = again.elements[0] else { Issue.record("not a heading"); return }
    #expect(heading.text == .textLiteral(SampleTextLiteral(text: "now a heading")) && heading.level == nil)
  }

  @Test(.timeLimit(.minutes(1)))
  func aSampleWrittenWithTypedElementsReadsBackAndAnElementCanChangeItsVariant() async throws {
    _ = try await openSampleRig()
    let button = SampleElement.button(SampleButton(id: "b1", children: [], press: .openProfile(SampleOpenProfileAction(userId: "u1")), label: .textLiteral(SampleTextLiteral(text: "Go"))))
    let pressable = SampleElement.pressable(SamplePressable(id: "p1", children: ["b1"]))
    let spec = SampleSpec(root: "b1", elements: [button, pressable, .unknown(typename: "SampleFuture")], state: [])
    let created = try await InterfaceRecord.create(slug: "local", spec: spec).optimisticValue!
    #expect(created.spec?.elements == [button, pressable, .unknown(typename: "SampleFuture")])

    let spacer = SampleElement.spacer(SampleSpacer(id: "b1", children: [], length: .numberLiteral(SampleNumberLiteral(number: 2))))
    let changed = try await created.update(spec: SampleSpec(root: "b1", elements: [spacer], state: [])).optimisticValue!
    #expect(changed.spec?.elements == [spacer])
    #expect(try await InterfaceRecord.find(created.id).current()?.spec?.elements == [spacer])
  }
}
