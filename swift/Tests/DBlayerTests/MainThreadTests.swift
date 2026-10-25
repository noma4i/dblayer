import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

final class MainThreadReads: @unchecked Sendable {
  static let shared = MainThreadReads()
  private let lock = NSLock()
  private var all = 0
  private var main = 0

  var counts: (all: Int, main: Int) { lock.withLock { (all, main) } }

  func touch() {
    let onMain = Thread.isMainThread
    lock.withLock { all += 1; if onMain { main += 1 } }
  }
}

struct ProbeRow: DBModel {
  static let modelName = "Probe"
  static let wireLayout = "probe"
  let key: String
  var id: String { MainThreadReads.shared.touch(); return key }
  let sync = SyncInfo()
  let upload: UploadInfo? = nil
  static let compared = MainThreadReads()

  init(key: String) { self.key = key }
  init(wire: WireRow) throws { key = wire.id }

  static func == (left: ProbeRow, right: ProbeRow) -> Bool { compared.touch(); return left.key == right.key }
}

final class SnapshotBox: @unchecked Sendable {
  var emit: (@Sendable (Snapshot<ProbeRow>) -> Void)?
}

struct CachedProbe: DBModel {
  static let modelName = "Moment"
  static let wireLayout = Moment.wireLayout
  static let reads = MainThreadReads()
  let id: String
  let sync = SyncInfo()
  let upload: UploadInfo? = nil

  init(wire: WireRow) throws { id = wire.id; Self.reads.touch() }
}

struct ProbeAnswer: Decodable, Sendable {
  static let reads = MainThreadReads()
  init(from _: Decoder) throws { Self.reads.touch() }
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aCallDecodesItsAnswerOffTheCallerThread() async throws {
    let transport = MockTransport()
    transport.enqueue("QrCodeCreate", reply: #"{"data":{"qrCodeCreate":{"url":"https://q"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"QrCodeCreate","document":"mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }"}]"#)

    _ = try await DB.run("QrCodeCreate", variables: ["input": .integer(1)], as: ProbeAnswer.self)

    let reads = ProbeAnswer.reads.counts
    #expect(reads.all == 1, "the probe saw the decode")
    #expect(reads.main == 0)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aWarmQueryDecodesItsCachedRowsOffTheCallerThread() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    for index in 0 ..< 2 { _ = try await Moment.create(text: "t\(index)") }
    let query = Query<CachedProbe>().order(Order(field: "text", direction: "asc"))
    try await query.warm()

    let cached = try await query.cached()
    let row = try await query.cachedRow(try #require(cached).value[0].id)

    #expect(cached?.value.count == 2)
    #expect(row != nil)
    let reads = CachedProbe.reads.counts
    #expect(reads.all == 3)
    #expect(reads.main == 0)
  }
}

@MainActor
struct MainThreadTests {
  @Test
  func aLiveListPreparesItsIdsOffTheMainThread() async {
    let rows = (0 ..< 2).map { ProbeRow(key: "r\($0)") }
    let source = LiveSource<Snapshot<ProbeRow>>(initial: Snapshot(rows: [], replaced: [:]), merge: { _, newer in newer }) { emit in
      emit(Snapshot(rows: rows, replaced: [:], reordered: true))
      return {}
    }
    let list = LiveList<ProbeRow>(Query<ProbeRow>(), snapshots: source)

    await eventually("the ids") { list.ids.count == 2 }

    let reads = MainThreadReads.shared.counts
    #expect(reads.all > 0, "the probe saw the ids prepared")
    #expect(reads.main == 0)
  }

  @Test
  func aLiveListPublishesItsRowsWithoutComparingThemOnTheMainThread() async {
    let box = SnapshotBox()
    let source = LiveSource<Snapshot<ProbeRow>>(initial: Snapshot(rows: [], replaced: [:]), merge: { _, newer in newer }) { emit in
      box.emit = emit
      return {}
    }
    let list = LiveList<ProbeRow>(Query<ProbeRow>(), snapshots: source)
    await eventually("the subscription") { box.emit != nil }
    let rows = (0 ..< 3).map { ProbeRow(key: "r\($0)") }

    box.emit?(Snapshot(rows: Array(rows.prefix(2)), replaced: [:], reordered: true))
    await eventually("the first delivery") { list.rows.count == 2 }
    box.emit?(Snapshot(rows: rows, replaced: [:], reordered: true))
    await eventually("the second delivery") { list.rows.count == 3 }

    let copy = rows.map { ProbeRow(key: $0.key) }
    let held = list.rows
    #expect(await Task.detached { copy == held }.value, "the probe sees a comparison of rows")
    let compared = ProbeRow.compared.counts
    #expect(compared.all > 0)
    #expect(compared.main == 0)
  }
}
