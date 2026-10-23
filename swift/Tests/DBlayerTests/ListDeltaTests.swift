import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  fileprivate static let byText = Query<Moment>().order(Order(field: "text", direction: "asc"))

  fileprivate static func open() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
  }

  fileprivate static func watching(_ query: Query<Moment>, feed: ListFeed<Moment>) async throws -> LiveStream<[Moment]> {
    let json = query.json
    let source = LiveSource<[Moment]>(initial: []) { emit in
      let subscription = try DB.database.liveQuery(queryJson: json, listener: ChangeListener { emit(feed.apply($0).rows) })
      return { subscription.cancel() }
    }
    return try await source.stream()
  }

  @Test
  func inv_observe_15_theFeedRebuildsTheListThatAFreshQueryReturns() async throws {
    try await Self.open()
    let feed = ListFeed<Moment>()
    let changes = Cursor(try await Self.watching(Self.byText, feed: feed))
    #expect(await changes.first("the empty list") { $0.isEmpty } != nil)
    var rows: [Moment] = []
    var seed: UInt64 = 0x9E3779B97F4A7C15
    func below(_ bound: Int) -> Int {
      seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17
      return Int(seed % UInt64(bound))
    }
    for step in 0 ..< 40 {
      switch (rows.isEmpty ? 0 : below(3)) {
      case 0: rows.append(try await Moment.create(text: String(format: "n%02d-%03d", below(40), step)))
      case 1:
        let at = below(rows.count)
        rows[at] = try await rows[at].update(text: String(format: "n%02d-%03d", below(40), step))
      default: try await rows.remove(at: below(rows.count)).destroy()
      }
      let fresh = try await Self.byText.live.current()
      #expect(await changes.first("the list of step \(step)", timeout: .seconds(20)) { $0 == fresh } != nil)
    }
  }

  @Test
  func inv_observe_15_aSnapshotMarksAReorderOnlyWhenTheIdsChange() async throws {
    try await Self.open()
    let first = try await Moment.create(text: "a")
    _ = try await Moment.create(text: "b")
    let snapshots = Cursor(try await Self.byText.snapshots.stream())
    #expect(await snapshots.first("both rows") { $0.rows.count == 2 } != nil)
    _ = try await first.update(text: "ax")
    let edited = await snapshots.first("the edited row") { $0.rows.first?.text == "ax" }
    #expect(edited?.reordered == false)
    _ = try await first.update(text: "z")
    let moved = await snapshots.first("the moved row") { $0.rows.map(\.text) == ["b", "z"] }
    #expect(moved?.reordered == true)
  }

  fileprivate static func moment(_ id: String) -> WireRow {
    let cells: [Cell] = [.text(value: id), .null, .text(value: id), .bool(value: false), .keyed(entries: []), .null, .text(value: "x")]
    return WireRow(id: id, revision: 1, estimatedBytes: 1, layout: Moment.wireLayout, cells: cells, sync: WireSync(state: .synced, actions: [], dirtyFields: [], error: nil, attempts: 0, since: 0), upload: nil)
  }

  fileprivate static func brokenMoment(_ id: String) -> WireRow {
    var row = moment(id)
    row.cells = []
    return row
  }

  fileprivate static func wire(full: Bool = false, size: UInt32, rows: [WireRow] = [], indexes: [UInt32] = [], removed: [String] = [], placed: [Placement] = [], renamed: [Replacement] = [], error: DbError? = nil) -> LiveChange {
    LiveChange(revision: 1, full: full, size: size, rows: rows, rowIndexes: indexes, removed: removed, placed: placed, replaced: [], renamed: renamed, count: nil, error: error)
  }

  fileprivate static func session(
    maxReopens: Int = 2, opens: @escaping @Sendable (Int) throws -> Void = { _ in }
  ) -> (session: ListSession<Moment, Snapshot<Moment>>, listeners: Cursor<AsyncStream<QueryListener>>, values: Cursor<AsyncStream<Snapshot<Moment>>>) {
    let (listeners, openings) = AsyncStream<QueryListener>.makeStream()
    let (values, sink) = AsyncStream<Snapshot<Moment>>.makeStream()
    let count = OpenCounter()
    let session = ListSession<Moment, Snapshot<Moment>>(
      maxReopens: maxReopens,
      open: { listener in
        try opens(count.next())
        openings.yield(listener)
        return {}
      },
      emit: { sink.yield($0) },
      map: { applied, _ in Snapshot(rows: applied.rows, replaced: [:], reordered: applied.reordered, error: applied.failure) }
    )
    return (session, Cursor(listeners), Cursor(values))
  }

  @Test
  func inv_observe_15_aDeliveryThatDoesNotFitTheRowsIsRefusedAndTheFeedCountsIt() {
    let feed = ListFeed<Moment>()
    #expect(feed.apply(Self.wire(full: true, size: 2, rows: ["a", "b"].map(Self.moment))).desynced == false)
    #expect(feed.apply(Self.wire(size: 3, placed: [Placement(id: "zz", index: 0)])).desynced)
    #expect(feed.apply(Self.wire(size: 2, rows: [Self.moment("a")], indexes: [5])).desynced)
    #expect(feed.apply(Self.wire(size: 9, rows: [Self.moment("a")], indexes: [0])).desynced)
    #expect(feed.apply(Self.wire(size: 2, removed: ["a"], placed: [Placement(id: "zz", index: 0)])).desynced)
    #expect(feed.rows.map(\.id) == ["a", "b"], "a refused delivery changes nothing")
    #expect(feed.desyncs == 4)
  }

  @Test
  func inv_observe_15_aDeliveryThatDoesNotFitOpensTheSubscriptionAgainForTheWholeList() async throws {
    let rig = Self.session()
    try rig.session.start()
    let first = try #require(await rig.listeners.next("the first subscription"))
    first.changed(change: Self.wire(full: true, size: 2, rows: ["a", "b"].map(Self.moment)))
    #expect(await rig.values.next("the first list")?.rows.map(\.id) == ["a", "b"])
    first.changed(change: Self.wire(size: 3, placed: [Placement(id: "zz", index: 0)]))
    let second = try #require(await rig.listeners.next("the second subscription"))
    second.changed(change: Self.wire(full: true, size: 3, rows: ["a", "b", "c"].map(Self.moment)))
    #expect(await rig.values.next("the whole list")?.rows.map(\.id) == ["a", "b", "c"])
    #expect(rig.session.desyncs == 1 && rig.session.reopened == 1)
  }

  @Test
  func inv_observe_15_aRowThatDoesNotDecodeKeepsItsSlotAndNeverReopensTheSubscription() async throws {
    let rig = Self.session()
    try rig.session.start()
    let listener = try #require(await rig.listeners.next("the subscription"))
    listener.changed(change: Self.wire(full: true, size: 3, rows: [Self.moment("a"), Self.brokenMoment("b"), Self.moment("c")]))
    #expect(await rig.values.next("the list without the bad row")?.rows.map(\.id) == ["a", "c"])
    for step in 0 ..< 3 {
      listener.changed(change: Self.wire(size: 3, rows: [Self.moment("c")], indexes: [2]))
      #expect(await rig.values.next("the update \(step)")?.rows.map(\.id) == ["a", "c"])
    }
    #expect(rig.session.reopened == 0 && rig.session.desyncs == 0)
  }

  @Test
  func inv_observe_18_aDeliveryThatInsertsTheRealRowNamesTheTemporaryIdItReplaced() {
    let feed = ListFeed<Moment>()
    #expect(feed.apply(Self.wire(full: true, size: 1, rows: [Self.moment("a")])).desynced == false)
    let change = Self.wire(size: 2, rows: [Self.moment("m77")], indexes: [1], placed: [Placement(id: "m77", index: 1)], renamed: [Replacement(from: "tmp-1", to: "m77")])
    #expect(feed.apply(change).desynced == false)
    #expect(change.replacedIds == ["m77": "tmp-1"])
  }

  @Test(.timeLimit(.minutes(1)))
  func inv_observe_18_aSnapshotNamesTheReplacedIdWhenTheTemporaryRowWasNeverDelivered() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    let gate = DispatchSemaphore(value: 0)
    let deliveries = OpenCounter()
    let json = Moment.recent.json
    let (values, sink) = AsyncStream<Snapshot<Moment>>.makeStream()
    let session = ListSession<Moment, Snapshot<Moment>>(
      open: { listener in
        let subscription = try DB.database.liveQuery(queryJson: json, listener: ChangeListener { change in
          let held = deliveries.next() == 0
          listener.changed(change: change)
          if held { gate.wait() }
        })
        return { subscription.cancel() }
      },
      emit: { sink.yield($0) },
      map: { applied, change in Snapshot(rows: applied.rows, replaced: change.replacedIds, reordered: applied.reordered) }
    )
    try session.start()
    let snapshots = Cursor(values)
    #expect(await snapshots.next("the empty list")?.rows.isEmpty == true)
    let local = try await Moment.create(text: "hello", media: nil)
    try await local.settled()
    gate.signal()
    let replaced = await snapshots.first("the snapshot that names the replaced id") { $0.replaced["m77"] != nil }?.replaced["m77"]
    #expect(replaced == local.id)
    session.cancel()
  }

  struct DesyncScenario: Sendable, CustomTestStringConvertible {
    let name: String
    let rounds: Int
    let goodDeltaFirst: Bool
    let reopenFails: Bool
    let stalls: Bool
    let reopened: Int?
    var testDescription: String { name }
  }

  @Test(arguments: [
    DesyncScenario(name: "keeps desyncing", rounds: 3, goodDeltaFirst: false, reopenFails: false, stalls: true, reopened: 2),
    DesyncScenario(name: "a good delta resets the count", rounds: 3, goodDeltaFirst: true, reopenFails: false, stalls: false, reopened: 3),
    DesyncScenario(name: "the reopen cannot open", rounds: 1, goodDeltaFirst: false, reopenFails: true, stalls: true, reopened: nil),
  ])
  func inv_observe_15_aDesyncedSubscriptionOpensAgainUntilItStallsAndSaysWhy(_ scenario: DesyncScenario) async throws {
    let rig = scenario.reopenFails ? Self.session(opens: { number in if number > 0 { throw URLError(.cannotConnectToHost) } }) : Self.session()
    try rig.session.start()
    var current = try #require(await rig.listeners.next("the first subscription"))
    for round in 0 ..< scenario.rounds {
      current.changed(change: Self.wire(full: true, size: 1, rows: [Self.moment("a")]))
      #expect(await rig.values.next("the list of round \(round)")?.rows.map(\.id) == ["a"])
      if scenario.goodDeltaFirst {
        current.changed(change: Self.wire(size: 1, rows: [Self.moment("a")], indexes: [0]))
        #expect(await rig.values.next("the good delta of round \(round)")?.rows.map(\.id) == ["a"])
      }
      current.changed(change: Self.wire(size: 2, placed: [Placement(id: "zz", index: 0)]))
      if scenario.stalls && round == scenario.rounds - 1 {
        let stopped = await rig.values.next("the failure")
        #expect(stopped?.error != nil && stopped?.rows.map(\.id) == ["a"])
      } else {
        current = try #require(await rig.listeners.next("the subscription after round \(round)"))
      }
    }
    #expect(rig.session.isStalled == scenario.stalls)
    if let reopened = scenario.reopened { #expect(rig.session.reopened == reopened) }
  }
}

final class OpenCounter: @unchecked Sendable {
  private let lock = NSLock()
  private var value = 0
  func next() -> Int { lock.withLock { defer { value += 1 }; return value } }
}
