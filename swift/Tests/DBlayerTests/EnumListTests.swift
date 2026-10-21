import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests.EnumTests {
  private func openShelves() async throws -> Database {
    try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
  }

  @Test @MainActor
  func aListOfEnumValuesIsStoredReadBackAndKeepsAValueTheDeclarationLacks() async throws {
    let database = try await openShelves()
    let wire = try await database.create(model: "Chat", valuesJson: #"{"title":"t","shelves":["PRIMARY","FUTURE"]}"#, argsJson: "{}")
    let chat = try Chat.decode(wire)
    #expect(chat.shelves == [.primary, .unknown("FUTURE")])
  }

  @Test @MainActor
  func aListOfEnumValuesWrittenAsTextsReadsBackTypedAndAnAbsentListReadsAsNil() async throws {
    let database = try await openShelves()
    let create: (String, [ChatShelf]?) async throws -> Chat = { try await Chat.create(title: $0, shelves: $1) }
    _ = create
    let typed = JSONValue.object(["title": .string("t"), "shelves": JSONValue.encode([ChatShelf.secondary])])
    #expect(try Chat.decode(try await database.create(model: "Chat", valuesJson: typed.text, argsJson: "{}")).shelves == [.secondary])
    #expect(try Chat.decode(try await database.create(model: "Chat", valuesJson: #"{"title":"u"}"#, argsJson: "{}")).shelves == nil)
  }

  @Test
  func aListOfEnumValuesEncodesAsTheirTexts() throws {
    let text = String(decoding: try JSONEncoder().encode([ChatShelf.primary, .unknown("x")]), as: UTF8.self)
    #expect(text == #"["PRIMARY","x"]"#)
  }

  @Test
  func aScopeParamOfASchemaEnumIsSentAsItsText() {
    #expect(Inbox.shelf(list: .primary).relation == "shelf")
    #expect(Inbox.shelf(list: .primary).params == ["list": .string("PRIMARY")])
    #expect(Inbox.shelf(list: .secondary).params != Inbox.shelf(list: .primary).params)
    #expect(Inbox.shelf(list: .unknown("LATER")).params == ["list": .string("LATER")])
  }
}
