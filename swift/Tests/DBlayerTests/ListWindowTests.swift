import DBlayerTesting
import Foundation
import Observation
import Testing
@testable import DBlayer

private final class WindowSnapshots: @unchecked Sendable {
  private let lock = NSLock()
  private var receive: (@Sendable (Snapshot<Moment>) -> Void)?
  var emit: (@Sendable (Snapshot<Moment>) -> Void)? {
    get { lock.withLock { receive } }
    set { lock.withLock { receive = newValue } }
  }
}

extension BridgeProbeTests {
  fileprivate static let byText = Query<Moment>().order(Order(field: "text", direction: "asc"))

  fileprivate static func openWindowBase() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
  }

  @Test @MainActor
  func aLiveWindowFollowsTheTailAndExpandsBefore() async throws {
    try await Self.openWindowBase()
    for number in 0 ..< 30 { _ = try await Moment.create(text: String(format: "t%05d", number)) }
    let live = LiveWindow(Self.byText.window(tail: 5))
    await eventually("the window") { live.rows.count == 5 }
    #expect(live.rows.map(\.text) == (25 ..< 30).map { String(format: "t%05d", $0) })
    #expect(live.hasBefore && !live.hasAfter && live.total == 30)
    #expect(live.snapshot.rows.map(\.id) == live.rows.map(\.id))
    #expect(live.snapshot.window?.total == live.total)
    var held = live.rows
    _ = live.takeUpdate()

    _ = try await Moment.create(text: "t00030")
    await eventually("the new tail row") { live.rows.last?.text == "t00030" }
    held.replay(live.takeUpdate().edits ?? [])
    #expect(held.map(\.id) == live.rows.map(\.id))
    #expect(live.total == 31)

    live.expand(before: 3)
    await eventually("the wider window") { live.rows.count == 8 }
    #expect(live.snapshot.rows.map(\.id) == live.rows.map(\.id))
    held.replay(live.takeUpdate().edits ?? [])
    #expect(held.map(\.id) == live.rows.map(\.id))
    live.expand(before: 100)
    await eventually("every row") { live.rows.count == 31 }
    #expect(!live.hasBefore && !live.hasAfter && live.total == 31)
  }

  @Test @MainActor
  func aRelatedWindowReadsTheSendersOfTheWindowRowsOnly() async throws {
    try await Self.openWindowBase()
    for (id, name) in [("u1", "Ann"), ("u2", "Bob"), ("u3", "Cy")] { _ = try await User.create(values: ["id": .string(id), "name": .string(name)]) }
    for (number, user) in ["u2", "u2", "u1", "u3"].enumerated() {
      _ = try await Moment.create(values: ["text": .string(String(format: "t%05d", number)), "userId": .string(user)])
    }
    let senders = Self.byText.window(tail: 2).related(Moment.Relations.author)
    #expect(try await senders.live.current().map(\.name) == ["Ann", "Cy"])
  }

  @Test @MainActor
  func aWindowThatGrowsPastMaxShrinksToItsFirstSize() async throws {
    try await Self.openWindowBase()
    for number in 0 ..< 30 { _ = try await Moment.create(text: String(format: "t%05d", number)) }
    let live = LiveWindow(Self.byText.window(tail: 5, max: 8))
    await eventually("the window") { live.rows.count == 5 }
    live.expand(before: 3)
    await eventually("the widest window") { live.rows.count == 8 }
    live.expand(before: 1)
    await eventually("the shrunk window") { live.rows.count == 5 }
    #expect(live.rows.map(\.text) == (25 ..< 30).map { String(format: "t%05d", $0) })
  }

  @Test @MainActor
  func aLiveWindowContentChangeOnlyInvalidatesItsRows() async throws {
    let sink = WindowSnapshots()
    let source = LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:])) { emit in sink.emit = emit; return {} }
    let live = LiveWindow(Self.byText.window(tail: 1), snapshots: source)
    await eventually("the window source") { sink.emit != nil }
    let before = Moment(id: "one", uuid: "one", text: "before", media: .object([:]), createdAt: "x")
    let after = Moment(id: "one", uuid: "one", text: "after", media: .object([:]), createdAt: "x")
    let edges = WindowEdges(LiveWindowEdge(hasBefore: true, hasAfter: false, total: 20))
    let error = NSError(domain: "window", code: 1)
    sink.emit?(Snapshot(rows: [before], replaced: [:], reordered: true, window: edges, seq: 1, phase: .loaded, error: error))
    await eventually("the initial snapshot") { live.snapshot.seq == 1 }
    let ids = Counter(), edgeChanges = Counter(), failures = Counter(), replacements = Counter(), rows = Counter()
    withObservationTracking { _ = live.ids } onChange: { ids.add() }
    withObservationTracking { _ = live.hasBefore; _ = live.hasAfter; _ = live.total } onChange: { edgeChanges.add() }
    withObservationTracking { _ = live.failure } onChange: { failures.add() }
    withObservationTracking { _ = live.replaced } onChange: { replacements.add() }
    withObservationTracking { _ = live.rows } onChange: { rows.add() }
    sink.emit?(Snapshot(rows: [after], replaced: [:], window: edges, seq: 2, phase: .loaded, error: error))
    await eventually("the changed content") { live.snapshot.seq == 2 }
    #expect(live.rows.first?.text == "after")
    #expect(rows.value == 1)
    #expect(ids.value == 0 && edgeChanges.value == 0 && failures.value == 0 && replacements.value == 0)
    let other = Moment(id: "two", uuid: "two", text: "last", media: .object([:]), createdAt: "x")
    sink.emit?(Snapshot(rows: [other], replaced: ["two": "one"], reordered: true, window: WindowEdges(LiveWindowEdge(hasBefore: false, hasAfter: true, total: 30)), seq: 3, phase: .loaded, error: NSError(domain: "window", code: 2)))
    await eventually("the changed order and state") { live.snapshot.seq == 3 }
    #expect(ids.value == 1 && edgeChanges.value == 1 && failures.value == 1 && replacements.value == 1)
    #expect(live.ids == ["two"] && live.total == 30)
  }

  @Test @MainActor
  func aLiveWindowKeepsItsEdgesWhenTheSnapshotHasNone() async throws {
    let sink = WindowSnapshots()
    let source = LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:])) { emit in sink.emit = emit; return {} }
    let live = LiveWindow(Self.byText.window(tail: 1), snapshots: source)
    await eventually("the window source") { sink.emit != nil }
    let row = Moment(id: "one", uuid: "one", media: .object([:]), createdAt: "x")
    sink.emit?(Snapshot(rows: [row], replaced: [:], reordered: true, window: WindowEdges(LiveWindowEdge(hasBefore: true, hasAfter: true, total: 20)), seq: 1, phase: .loaded))
    await eventually("the initial edges") { live.snapshot.seq == 1 }
    sink.emit?(Snapshot(rows: [row], replaced: [:], seq: 2, phase: .loaded))
    await eventually("the window-less snapshot") { live.snapshot.seq == 2 }
    #expect(live.hasBefore && live.hasAfter && live.total == 20)
  }
}
