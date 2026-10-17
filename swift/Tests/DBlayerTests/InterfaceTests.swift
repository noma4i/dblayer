import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let screenDocument = """
  query ScreenNode($id: ID!) { node(id: $id) { ... on CustomScreen { id slug spec { root \
  elements { __typename id children press { __typename ... on ScreenNavigateAction { slug title } ... on ScreenOpenChatAction { chatId } ... on ScreenOpenExternalAction { url } ... on ScreenOpenProfileAction { userId } } \
  visible { __typename ... on ScreenCondition { comparison negated operand { __typename ... on ScreenTextLiteral { text } ... on ScreenStateRef { path } } path } ... on ScreenVisibleAll { conditions { comparison negated path } } ... on ScreenVisibleFlag { flag } } \
  ... on ScreenButton { label { __typename ... on ScreenStateRef { path } ... on ScreenTextLiteral { text } } disabled { __typename ... on ScreenFlagLiteral { flag } } } \
  ... on ScreenColumn { gap { __typename ... on ScreenNumberLiteral { number } } alignItems } ... on ScreenHeading { text { __typename ... on ScreenTextLiteral { text } } level } \
  ... on ScreenSpacer { length { __typename ... on ScreenNumberLiteral { number } } } ... on ScreenSwitch { checked { __typename ... on ScreenStateRef { path } } label { __typename ... on ScreenTextLiteral { text } } } } \
  state { path value { __typename ... on ScreenTextLiteral { text } ... on ScreenNumberLiteral { number } } } } } } }
  """

  private static let screenElements = #"""
  [
    {"__typename":"ScreenColumn","id":"root","children":["b1","h1","p1","sw1","sp1","f1"],"press":null,"visible":{"__typename":"ScreenVisibleAll","conditions":[{"comparison":"EQ","negated":false,"operand":{"__typename":"ScreenTextLiteral","text":"x"},"path":"user.name"}]},"gap":{"__typename":"ScreenNumberLiteral","number":8},"alignItems":"center"},
    {"__typename":"ScreenButton","id":"b1","children":[],"press":{"__typename":"ScreenNavigateAction","slug":"home","title":"Home"},"visible":null,"label":{"__typename":"ScreenStateRef","path":"user.name"},"disabled":{"__typename":"ScreenFlagLiteral","flag":true}},
    {"__typename":"ScreenHeading","id":"h1","children":[],"press":null,"visible":{"__typename":"ScreenVisibleFlag","flag":true},"text":{"__typename":"ScreenTextLiteral","text":"Hi"},"level":2},
    {"__typename":"ScreenPressable","id":"p1","children":["h1"],"press":{"__typename":"ScreenOpenChatAction","chatId":"c9"},"visible":{"__typename":"ScreenCondition","comparison":"NE","negated":true,"operand":{"__typename":"ScreenStateRef","path":"flag"},"path":"a.b"}},
    {"__typename":"ScreenSwitch","id":"sw1","children":[],"press":null,"visible":null,"checked":{"__typename":"ScreenStateRef","path":"on"},"label":null},
    {"__typename":"ScreenSpacer","id":"sp1","children":[],"press":null,"visible":null,"length":{"__typename":"ScreenNumberLiteral","number":4.5}},
    {"__typename":"ScreenFuture","id":"f1","children":["x"],"size":3}
  ]
  """#

  private static func screenReply(elements: String, state: String = #"[{"path":"user.name","value":{"__typename":"ScreenTextLiteral","text":"Ada"}},{"path":"n","value":{"__typename":"ScreenNumberLiteral","number":3}}]"#) -> String {
    #"{"data":{"node":{"__typename":"CustomScreen","id":"s1","slug":"main","spec":{"root":"root","elements":\#(elements),"state":\#(state)}}}}"#
  }

  private func openScreenRig() throws -> MockTransport {
    let transport = MockTransport()
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ScreenNode"), "document": .string(Self.screenDocument)])]).text)
    return transport
  }

  @Test(.timeLimit(.minutes(1)))
  func aServerAnswerCarriesEveryElementVariantAndALaterAnswerReplacesTheList() async throws {
    let transport = try openScreenRig()
    transport.enqueue("ScreenNode", reply: Self.screenReply(elements: Self.screenElements))
    let screen = try #require(try await CustomScreen.node("s1"))
    let spec = try #require(screen.spec)
    #expect(spec.root == "root" && spec.elements.count == 7)
    #expect(spec.elements.map(\.id) == ["root", "b1", "h1", "p1", "sw1", "sp1", nil])
    #expect(spec.elements.map(\.kind) == [.column, .button, .heading, .pressable, .switch, .spacer, nil])
    #expect(spec.elements[0].children == ["b1", "h1", "p1", "sw1", "sp1", "f1"])
    #expect(spec.elements[3].children == ["h1"])
    #expect(spec.elements[6] == .unknown(typename: "ScreenFuture"))
    guard case let .button(button) = spec.elements[1] else { Issue.record("not a button"); return }
    #expect(button.press == .navigate(ScreenNavigateAction(slug: "home", title: "Home")))
    #expect(button.label == .stateRef(ScreenStateRef(path: "user.name")) && button.disabled == .flagLiteral(ScreenFlagLiteral(flag: true)))
    guard case let .column(column) = spec.elements[0] else { Issue.record("not a column"); return }
    #expect(column.gap == .numberLiteral(ScreenNumberLiteral(number: 8)) && column.alignItems == "center")
    #expect(column.visible == .visibleAll(ScreenVisibleAll(conditions: [ScreenCondition(comparison: "EQ", negated: false, operand: .textLiteral(ScreenTextLiteral(text: "x")), path: "user.name")])))
    #expect(spec.elements[1].press == .navigate(ScreenNavigateAction(slug: "home", title: "Home")))
    #expect(spec.elements[3].press == .openChat(ScreenOpenChatAction(chatId: "c9")))
    #expect(spec.elements[3].visible == .condition(ScreenCondition(comparison: "NE", negated: true, operand: .stateRef(ScreenStateRef(path: "flag")), path: "a.b")))
    #expect(spec.elements[2].visible == .visibleFlag(ScreenVisibleFlag(flag: true)))
    #expect(spec.state == [ScreenStateEntry(path: "user.name", value: .text(ScreenTextLiteral(text: "Ada"))), ScreenStateEntry(path: "n", value: .number(ScreenNumberLiteral(number: 3)))])

    let second = #"""
    [
      {"__typename":"ScreenHeading","id":"b1","children":["q"],"press":{"__typename":"ScreenOpenExternalAction","url":"https://x"},"visible":null,"text":{"__typename":"ScreenTextLiteral","text":"now a heading"},"level":null},
      {"__typename":"ScreenColumn","id":"root","children":["b1"],"press":null,"visible":null,"gap":null,"alignItems":null}
    ]
    """#
    transport.enqueue("ScreenNode", reply: Self.screenReply(elements: second, state: "[]"))
    _ = try await CustomScreen.node("s1", force: true)
    let again = try #require(await CustomScreen.find("s1").current()?.spec)
    #expect(again.elements.map(\.id) == ["b1", "root"])
    #expect(again.elements.map(\.kind) == [.heading, .column])
    #expect(again.elements[0].children == ["q"] && again.elements[0].press == .openExternal(ScreenOpenExternalAction(url: "https://x")))
    #expect(again.elements[1].visible == nil && again.state.isEmpty)
    guard case let .heading(heading) = again.elements[0] else { Issue.record("not a heading"); return }
    #expect(heading.text == .textLiteral(ScreenTextLiteral(text: "now a heading")) && heading.level == nil)
  }

  @Test(.timeLimit(.minutes(1)))
  func aScreenWrittenWithTypedElementsReadsBackAndAnElementCanChangeItsVariant() async throws {
    _ = try openScreenRig()
    let button = ScreenElement.button(ScreenButton(id: "b1", children: [], press: .openProfile(ScreenOpenProfileAction(userId: "u1")), label: .textLiteral(ScreenTextLiteral(text: "Go"))))
    let pressable = ScreenElement.pressable(ScreenPressable(id: "p1", children: ["b1"]))
    let spec = ScreenSpec(root: "b1", elements: [button, pressable, .unknown(typename: "ScreenFuture")], state: [])
    let created = try await CustomScreen.create(slug: "local", spec: spec)
    #expect(created.spec?.elements == [button, pressable, .unknown(typename: "ScreenFuture")])

    let spacer = ScreenElement.spacer(ScreenSpacer(id: "b1", children: [], length: .numberLiteral(ScreenNumberLiteral(number: 2))))
    let changed = try await created.update(spec: ScreenSpec(root: "b1", elements: [spacer], state: []))
    #expect(changed.spec?.elements == [spacer])
    #expect(await CustomScreen.find(created.id).current()?.spec?.elements == [spacer])
  }
}
