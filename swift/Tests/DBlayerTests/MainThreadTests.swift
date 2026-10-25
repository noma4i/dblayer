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
  let uploads: [String: UploadInfo] = [:]

  init(key: String) { self.key = key }
  init(wire: WireRow) throws { key = wire.id }
}

struct CachedProbe: DBModel {
  static let modelName = "Moment"
  static let wireLayout = Moment.wireLayout
  static let reads = MainThreadReads()
  let id: String
  let sync = SyncInfo()
  let uploads: [String: UploadInfo] = [:]

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

  @Test(arguments: ["list", "window"])
  func aLiveCollectionHasItsUpdateReadyWhileTheMainActorIsBusy(kind: String) async {
    let prepared = DispatchSemaphore(value: 0)
    let box = SnapshotBox()
    let source = LiveSource<Snapshot<PreparedRow>>(initial: Snapshot(rows: [], replaced: [:]), merge: { _, newer in newer }) { emit in
      box.set(emit)
      return {}
    }
    let take: () -> ListUpdate<PreparedRow>
    let shown: () -> Int
    if kind == "list" {
      let list = LiveList<PreparedRow>(Query<PreparedRow>(), snapshots: source)
      (take, shown) = ({ list.takeUpdate() }, { list.rows.count })
    } else {
      let window = LiveWindow<PreparedRow>(Window<PreparedRow>(query: Query<PreparedRow>(), tail: 3), snapshots: source)
      (take, shown) = ({ window.takeUpdate() }, { window.rows.count })
    }
    await eventually("the subscription") { box.get() != nil }
    let rows = (0 ..< 3).map { PreparedRow(key: "r\($0)", prepared: prepared) }
    box.get()?(Snapshot(rows: rows, replaced: [:], reordered: true, seq: 1))
    await eventually("the first delivery") { shown() == 3 }
    _ = take()
    while block(on: prepared, seconds: 0) {}

    box.get()?(Snapshot(rows: rows, replaced: [:], reordered: true, edits: [.rewrite(2, rows[2])], seq: 2))
    let preparedInTheBackground = block(on: prepared, seconds: 5)
    var update = take()
    let deadline = DispatchTime.now() + 5
    while update.revision < 2, DispatchTime.now() < deadline {
      usleep(1000)
      update = take()
    }

    #expect(preparedInTheBackground, "\(kind): the background stage prepared the delivery")
    #expect(update.revision == 2, "\(kind): the revision")
    #expect(update.edits?.count == 1, "\(kind): the edits")
    #expect(update.rows.count == 3, "\(kind): the rows")
  }
}

func block(on semaphore: DispatchSemaphore, seconds: Double) -> Bool {
  semaphore.wait(timeout: .now() + seconds) == .success
}

struct PreparedRow: DBModel {
  static let modelName = "Prepared"
  static let wireLayout = "prepared"
  let key: String
  let prepared: DispatchSemaphore
  var id: String { if !Thread.isMainThread { prepared.signal() }; return key }
  let sync = SyncInfo()
  let uploads: [String: UploadInfo] = [:]

  init(key: String, prepared: DispatchSemaphore) { self.key = key; self.prepared = prepared }
  init(wire: WireRow) throws { key = wire.id; prepared = DispatchSemaphore(value: 0) }
  static func == (left: PreparedRow, right: PreparedRow) -> Bool { left.key == right.key }
}

final class SnapshotBox: @unchecked Sendable {
  private let lock = NSLock()
  private var emit: (@Sendable (Snapshot<PreparedRow>) -> Void)?

  func set(_ next: @escaping @Sendable (Snapshot<PreparedRow>) -> Void) { lock.withLock { emit = next } }
  func get() -> (@Sendable (Snapshot<PreparedRow>) -> Void)? { lock.withLock { emit } }
}

struct ListUpdatesReleaseTests {
  final class Token: @unchecked Sendable {
    let released: () -> Void
    init(_ released: @escaping () -> Void) { self.released = released }
    deinit { released() }
  }

  struct TokenRow: DBModel {
    static let modelName = "Token"
    static let wireLayout = "token"
    let key: String
    let token: Token?
    var id: String { key }
    let sync = SyncInfo()
    let uploads: [String: UploadInfo] = [:]

    init(key: String, token: Token? = nil) { self.key = key; self.token = token }
    init(wire: WireRow) throws { key = wire.id; token = nil }
    static func == (left: TokenRow, right: TokenRow) -> Bool { left.key == right.key }
  }

  final class Outcome: @unchecked Sendable {
    private let lock = NSLock()
    private var checked = false
    private var blocked = false
    var seen: (checked: Bool, blocked: Bool) { lock.withLock { (checked, blocked) } }
    func begin() -> Bool { lock.withLock { defer { checked = true }; return !checked } }
    func block() { lock.withLock { blocked = true } }
  }

  @Test
  func theRowsOfAReplacedUpdateAreReleasedWithoutHoldingTheLock() {
    let updates = ListUpdates<TokenRow>()
    let outcome = Outcome()
    func publishRowWithToken() {
      let token = Token {
        guard outcome.begin() else { return }
        let done = DispatchSemaphore(value: 0)
        Thread.detachNewThread { _ = updates.take(); done.signal() }
        if done.wait(timeout: .now() + 2) == .timedOut { outcome.block() }
      }
      updates.publish(Snapshot(rows: [TokenRow(key: "a", token: token)], replaced: [:], seq: 1))
    }
    publishRowWithToken()

    updates.publish(Snapshot(rows: [TokenRow(key: "b")], replaced: [:], seq: 2))

    #expect(outcome.seen.checked, "the old rows were released by the publication")
    #expect(!outcome.seen.blocked, "a take had to wait for the release of the old rows")
  }
}
