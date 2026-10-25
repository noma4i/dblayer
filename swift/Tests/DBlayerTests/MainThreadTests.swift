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

  init(key: String) { self.key = key }
  init(wire: WireRow) throws { key = wire.id }
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
}
