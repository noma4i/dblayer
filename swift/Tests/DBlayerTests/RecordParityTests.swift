import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private struct ProbeValues: Equatable, Sendable {
  var text: String?
  var count: Int?
  var ratioBits: UInt64?
  var flag: Bool?
  var at: String?
  var blob: JSONValue?

  init(text: String? = nil, count: Int? = nil, ratio: Double? = nil, flag: Bool? = nil, at: String? = nil, blob: JSONValue? = nil) {
    self.text = text
    self.count = count
    ratioBits = ratio?.bitPattern
    self.flag = flag
    self.at = at
    self.blob = blob
  }

  init(_ row: ScalarProbe) {
    self.init(text: row.text, count: row.count, ratio: row.ratio, flag: row.flag, at: row.at, blob: row.blob)
  }

  var ratio: Double? { ratioBits.map(Double.init(bitPattern:)) }
}

private let wideBlob: JSONValue = .object([
  "max": .integer(Int64.max),
  "min": .integer(Int64.min),
  "beyondDouble": .integer(9_007_199_254_740_993),
  "real": .number(1.5),
  "flag": .bool(false),
  "none": .null,
  "list": .array([.integer(0), .string(""), .array([]), .object([:])]),
  "text": .string("日本 🎉"),
])

private let probeCases: [(String, ProbeValues)] = [
  ("none in every field", ProbeValues()),
  ("zero in every kind", ProbeValues(text: "", count: 0, ratio: 0.0, flag: false, at: "1970-01-01T00:00:00.000Z", blob: .integer(0))),
  ("upper bounds and unicode", ProbeValues(text: "\u{451}\u{436} 日本 🎉 e\u{301} \"q\" \\ \n\t", count: Int.max, ratio: Double.greatestFiniteMagnitude, flag: true, at: "9999-12-31T23:59:59.999Z", blob: wideBlob)),
  ("lower bounds and a long text", ProbeValues(text: String(repeating: "x", count: 65_536), count: Int.min, ratio: -Double.greatestFiniteMagnitude, flag: false, at: "0001-01-01T00:00:00.000Z", blob: .string(""))),
  ("smallest real and the first integer beyond a double", ProbeValues(count: 9_007_199_254_740_993, ratio: Double.leastNonzeroMagnitude, blob: .array([]))),
  ("negative integer and an empty object", ProbeValues(count: -1, ratio: -1.5, flag: true, blob: .object([:]))),
  ("a boolean json", ProbeValues(blob: .bool(false))),
]

private let fileCases: [(String, Data, String, String)] = [
  ("empty bytes", Data(), "empty.bin", "application/octet-stream"),
  ("one byte and a unicode name", Data([0]), "\u{444}\u{43E}\u{442}\u{43E} 🎉.png", "image/png"),
  ("one mebibyte", Data(repeating: 0xAB, count: 1 << 20), "big.bin", "application/octet-stream"),
]

struct RefusalCase: Sendable, CustomTestStringConvertible {
  let label: String
  let text: String
  let failure: DbError?
  let code: SessionCode?
  let oversize: Bool
  var testDescription: String { label }
}

private let refusalCases: [RefusalCase] = [
  RefusalCase(label: "a request over the size limit is refused locally with its size and limit", text: String(repeating: "x", count: 300_000), failure: nil, code: nil, oversize: true),
  RefusalCase(label: "a session code arrives without a size and a limit", text: "hello", failure: .Session(code: .rateLimited, detail: "slow down"), code: .rateLimited, oversize: false),
]

private let failureOperations = #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#

/// bridge_prop_record_parity (P-BR-05): a value written through the typed API comes back through the optimistic row, the row read and the snapshot stream as it was written.
extension BridgeProbeTests {
  private func create(_ values: ProbeValues) async throws -> MutationReceipt<ScalarProbe> {
    try await ScalarProbe.create(text: values.text, count: values.count, ratio: values.ratio, flag: values.flag, at: values.at, blob: values.blob)
  }

  private func update(_ row: ScalarProbe, to values: ProbeValues) async throws -> MutationReceipt<ScalarProbe> {
    try await row.update(text: .some(values.text), count: .some(values.count), ratio: .some(values.ratio), flag: .some(values.flag), at: .some(values.at), blob: .some(values.blob))
  }

  private func expectParity(_ id: String, _ optimistic: ScalarProbe?, _ expected: ProbeValues, _ label: String) async throws {
    let comment = Comment(rawValue: label)
    #expect(optimistic.map { ProbeValues($0) } == expected, comment)
    #expect(try await ScalarProbe.find(id).current().map { ProbeValues($0) } == expected, comment)
    let cursor = Cursor(try await ScalarProbe.ids([id]).snapshots.stream())
    let snapshot = await cursor.first("the snapshot of the row") { !$0.rows.isEmpty }
    #expect(snapshot?.rows.first.map { ProbeValues($0) } == expected, comment)
  }

  @Test(.timeLimit(.minutes(1)))
  func bridge_prop_record_parity_everyScalarKindKeepsItsValueThroughCreateExplicitNoneAndUpdate() async throws {
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: nil)
    for (label, values) in probeCases {
      let created = try #require(try await create(values).optimisticValue)
      try await expectParity(created.id, created, values, "create: \(label)")

      let cleared = try #require(try await update(created, to: ProbeValues()).optimisticValue)
      try await expectParity(created.id, cleared, ProbeValues(), "update to None: \(label)")

      let restored = try #require(try await update(cleared, to: values).optimisticValue)
      try await expectParity(created.id, restored, values, "update back: \(label)")
    }
  }

  /// The core keeps one zero: a negative zero is stored and read as the positive one, on both platforms (value_tests.rs number_edge_cases_normalize).
  @Test(.timeLimit(.minutes(1)))
  func bridge_prop_record_parity_aNegativeZeroIsStoredAsThePositiveZero() async throws {
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: nil)
    let created = try #require(try await create(ProbeValues(ratio: -0.0)).optimisticValue)
    #expect(created.ratio?.bitPattern == 0.0.bitPattern)
    #expect(try await ScalarProbe.find(created.id).current()?.ratio?.bitPattern == 0.0.bitPattern)
  }

  @Test(.timeLimit(.minutes(1)))
  func bridge_prop_record_parity_aFileKeepsItsSizeNameMimeAndDigestThroughAttachReadRowAndSnapshot() async throws {
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: nil)
    let probe = try #require(try await create(ProbeValues()).optimisticValue)
    #expect(try await probe.file(.file) == nil)
    for (label, bytes, name, mime) in fileCases {
      let comment = Comment(rawValue: label)
      let info = try await probe.attach(.file, data: bytes, name: name, mime: mime)
      #expect((info.name, info.mime, info.size) == (name, mime, UInt64(bytes.count)), comment)
      #expect(try await probe.file(.file) == info, comment)
      #expect(try await probe.read(.file) == bytes, comment)
      #expect(try await ScalarProbe.find(probe.id).current()?.file == info, comment)
      let cursor = Cursor(try await ScalarProbe.ids([probe.id]).snapshots.stream())
      let snapshot = await cursor.first("the snapshot of the row with the file") { $0.rows.first?.file == info }
      #expect(snapshot != nil, comment)
    }
    #expect(try await probe.detach(.file))
    #expect(try await ScalarProbe.find(probe.id).current()?.file == nil)
    let cursor = Cursor(try await ScalarProbe.ids([probe.id]).snapshots.stream())
    #expect(await cursor.first("the snapshot of the row without the file") { !$0.rows.isEmpty && $0.rows.first?.file == nil } != nil)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: refusalCases)
  func bridge_prop_record_parity_aRefusalKeepsTheSessionCodeTheRequestSizeAndTheLimitOrTheirAbsence(_ refusal: RefusalCase) async throws {
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: failureOperations)
    if let failure = refusal.failure { transport.enqueue("CreateMoment", failure: failure) }
    let refused = try await Moment.create(text: refusal.text, media: nil, in: DB.account()).refusal()
    let comment = Comment(rawValue: refusal.label)
    #expect(refused.sessionCode == refusal.code, comment)
    if refusal.oversize {
      #expect(refused.requestLimit == 256 * 1024 && (refused.requestSize ?? 0) > 256 * 1024, comment)
    } else {
      #expect(refused.requestSize == nil && refused.requestLimit == nil, comment)
    }
    #expect(!refused.message.isEmpty, comment)
    let restored = OperationFailure(wire: refused)
    #expect((restored.sessionCode, restored.requestSize, restored.requestLimit, restored.kind) == (refused.sessionCode, refused.requestSize, refused.requestLimit, refused.kind), comment)
    #expect(try JSONDecoder().decode(OperationFailure.self, from: JSONEncoder().encode(restored)) == restored, comment)
  }
}
