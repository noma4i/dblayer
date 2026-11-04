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
    _ = database
    let container = try await DB.account().send(Container.self, values: ["title": .string("t"), "shelves": .array([.string("PRIMARY"), .string("FUTURE")])]).optimisticValue!
    #expect(container.shelves == [.primary, .unknown("FUTURE")])
  }

  @Test @MainActor
  func aListOfEnumValuesWrittenAsTextsReadsBackTypedAndAnAbsentListReadsAsNil() async throws {
    let database = try await openShelves()
    let typed = JSONValue.object(["title": .string("t"), "shelves": try JSONValue.encode([ContainerShelf.secondary])])
    _ = database
    #expect(try await DB.account().send(Container.self, values: typed.objectValue).optimisticValue!.shelves == [.secondary])
    #expect(try await DB.account().send(Container.self, values: ["title": .string("u")]).optimisticValue!.shelves == nil)
  }

  @Test
  func aListOfEnumValuesEncodesAsTheirTexts() throws {
    let text = String(decoding: try JSONEncoder().encode([ContainerShelf.primary, .unknown("x")]), as: UTF8.self)
    #expect(text == #"["PRIMARY","x"]"#)
  }
}
