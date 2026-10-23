import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests.EnumTests {
  private func openShelves() async throws -> Database {
    try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
  }

  @Test @MainActor
  func aListOfEnumValuesIsStoredReadBackAndKeepsAValueTheDeclarationLacks() async throws {
    let database = try await openShelves()
    let wire = try await database.create(model: "Container", valuesJson: #"{"title":"t","shelves":["PRIMARY","FUTURE"]}"#, argsJson: "{}")
    let container = try Container.decode(wire)
    #expect(container.shelves == [.primary, .unknown("FUTURE")])
  }

  @Test @MainActor
  func aListOfEnumValuesWrittenAsTextsReadsBackTypedAndAnAbsentListReadsAsNil() async throws {
    let database = try await openShelves()
    let typed = JSONValue.object(["title": .string("t"), "shelves": JSONValue.encode([ContainerShelf.secondary])])
    #expect(try Container.decode(try await database.create(model: "Container", valuesJson: typed.text, argsJson: "{}")).shelves == [.secondary])
    #expect(try Container.decode(try await database.create(model: "Container", valuesJson: #"{"title":"u"}"#, argsJson: "{}")).shelves == nil)
  }

  @Test
  func aListOfEnumValuesEncodesAsTheirTexts() throws {
    let text = String(decoding: try JSONEncoder().encode([ContainerShelf.primary, .unknown("x")]), as: UTF8.self)
    #expect(text == #"["PRIMARY","x"]"#)
  }
}
