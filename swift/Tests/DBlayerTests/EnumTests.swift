import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
struct EnumTests {
  private func open() async throws -> Database {
    try await openDatabase(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: nil)
  }

  @Test
  func anUnknownValueDecodesWithoutLossAndEncodesBackAsItArrived() throws {
    let decoded = try JSONDecoder().decode([VibeKind].self, from: Data(#"["like","voice_note"]"#.utf8))
    #expect(decoded == [.like, .unknown("voice_note")])
    #expect(String(decoding: try JSONEncoder().encode(decoded), as: UTF8.self) == #"["like","voice_note"]"#)
    #expect(VibeKind(rawValue: "love") == .love && VibeKind.unknown("x").rawValue == "x")
  }

  @Test @MainActor
  func aValueOutsideTheDeclaredOnesIsStoredReadBackAndKeptByAnEdit() async throws {
    let database = try await open()
    try await adopt(database)
    let wire = try await database.create(epoch: database.sessionEpoch(), model: "NestedSample", valuesJson: #"{"phase":"archived"}"#, argsJson: "{}").optimisticValue!
    let sample = try NestedSample.decode(wire)
    #expect(sample.phase == .unknown("archived"))
    let edited = try await sample.update(values: []).optimisticValue!
    #expect(edited.phase == .unknown("archived"))
    let matching = Cursor(try await NestedSample.where(NestedSample.phase == .unknown("archived")).snapshots.stream())
    #expect(await matching.first("the row of the unknown value") { $0.rows.map(\.id) == [sample.id] } != nil)
    let declared = Cursor(try await NestedSample.where(NestedSample.phase == .live).snapshots.stream())
    #expect(await declared.first("no row of a declared value") { $0.rows.isEmpty } != nil)
  }

  @Test @MainActor
  func theTypedApiWritesADeclaredCaseAndQueriesCompareTypedValues() async throws {
    try await adopt(try await open())
    let sample = try await NestedSample.create(phase: .live).optimisticValue!
    #expect(sample.phase == .live)
    let live = Cursor(try await NestedSample.where(NestedSample.phase == .live).snapshots.stream())
    #expect(await live.first("the live row") { $0.rows.map(\.id) == [sample.id] } != nil)
    let changed = try await sample.update(phase: .unknown("later")).optimisticValue!
    #expect(changed.phase == .unknown("later"))
  }

  @Test
  func aNestedEnumKeepsAValueTheDeclarationLacksOnBothDecodePaths() async throws {
    let json = Data(#"{"title":"t","state":"mystery"}"#.utf8)
    #expect(try JSONDecoder().decode(NestedValue.self, from: json).state == .unknown("mystery"))
    let database = try await open()
    let wire = try await database.create(epoch: database.sessionEpoch(), model: "NestedSample", valuesJson: #"{"value":{"title":"t","state":"mystery"}}"#, argsJson: "{}").optimisticValue!
    let sample = try NestedSample.decode(wire)
    #expect(sample.value?.state == .unknown("mystery"))
    let encoded = String(decoding: try JSONEncoder().encode(sample.value), as: UTF8.self)
    #expect(encoded.contains(#""state":"mystery""#), "encoded: \(encoded)")
  }
}
}
