import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  fileprivate static let byText = Query<Moment>().order(Order(field: "text", direction: "asc"))

  fileprivate static func openWindowBase() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
  }

  @Test
  func aWindowDecodesOnlyItsRowsAtEveryListSize() async throws {
    var costs: [[Int]] = []
    for count in [20, 200, 2000] {
      try await Self.openWindowBase()
      var created: [Moment] = []
      for number in 0 ..< count { created.append(try await Moment.create(text: String(format: "t%05d", number))) }
      let feed = ListFeed<Moment>()
      let source = LiveSource<[Moment]>(initial: []) { emit in
        let subscription = try DB.database.liveWindow(queryJson: Self.byText.json, tail: 10, listener: ChangeListener { emit(feed.apply($0).rows) })
        return { subscription.cancel() }
      }
      let changes = Cursor(await source.stream())
      #expect(await changes.first("the window") { $0.count == 10 } != nil)
      let atStart = feed.decodedRows
      let edited = String(format: "t%05dx", count - 1)
      _ = try await created[count - 1].update(text: edited)
      #expect(await changes.first("the edited row") { $0.last?.text == edited } != nil)
      let afterInside = feed.decodedRows
      _ = try await created[0].update(text: "t00000x")
      let again = String(format: "t%05dy", count - 1)
      _ = try await created[count - 1].update(text: again)
      #expect(await changes.first("the second edit") { $0.last?.text == again } != nil)
      costs.append([atStart, afterInside - atStart, feed.decodedRows - afterInside])
    }
    #expect(costs == [[10, 1, 1], [10, 1, 1], [10, 1, 1]], "rows decoded for the window, 1 edit inside, 1 edit before plus 1 edit inside, at 20, 200, 2000 rows")
  }

  @Test @MainActor
  func aLiveWindowFollowsTheTailAndExpandsBefore() async throws {
    try await Self.openWindowBase()
    for number in 0 ..< 30 { _ = try await Moment.create(text: String(format: "t%05d", number)) }
    let live = LiveWindow(Self.byText.window(tail: 5))
    await eventually("the window") { live.rows.count == 5 }
    #expect(live.rows.map(\.text) == (25 ..< 30).map { String(format: "t%05d", $0) })
    #expect(live.hasBefore && !live.hasAfter && live.total == 30)
    var held = live.rows
    _ = live.takeEdits()

    _ = try await Moment.create(text: "t00030")
    await eventually("the new tail row") { live.rows.last?.text == "t00030" }
    held.replay(live.takeEdits() ?? [])
    #expect(held.map(\.id) == live.rows.map(\.id))
    #expect(live.total == 31)

    live.expand(before: 3)
    await eventually("the wider window") { live.rows.count == 8 }
    held.replay(live.takeEdits() ?? [])
    #expect(held.map(\.id) == live.rows.map(\.id))
    live.expand(before: 100)
    await eventually("every row") { live.rows.count == 31 }
    #expect(!live.hasBefore && !live.hasAfter && live.total == 31)
  }

  fileprivate static func threadNode(_ number: Int, text: String? = nil) -> String {
    #"{"__typename":"Record","id":"m\#(number)","containerId":"c1","authorId":"a","text":"\#(text ?? "t\(number)")","createdAt":"2026-10-0\#(number)T00:00:00.000Z","sequenceNumber":\#(number)}"#
  }

  fileprivate static func threadReply(_ nodes: [String]) -> String {
    #"{"data":{"container":{"records":{"nodes":[\#(nodes.joined(separator: ","))],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"#
  }

  @Test
  func aWindowOverTheThreadShapeDecodesOnlyItsRows() async throws {
    let transport = MockTransport()
    transport.enqueue("ContainerThread", reply: Self.threadReply((1 ... 7).map { Self.threadNode($0) }))
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerThread","document":"query ContainerThread($containerId: ID!, $first: Int, $after: String) { container(id: $containerId) { records(first: $first, after: $after) { nodes { id containerId authorId text createdAt sequenceNumber } pageInfo { hasNextPage endCursor } } } }"},{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt sequenceNumber } }"}]"#)
    let thread = Record.thread("c1")
    try await thread.load()
    let feed = ListFeed<Record>()
    let source = LiveSource<[Record]>(initial: []) { emit in
      let subscription = try DB.database.liveWindow(queryJson: thread.json, tail: 3, listener: ChangeListener { emit(feed.apply($0).rows) })
      return { subscription.cancel() }
    }
    let changes = Cursor(await source.stream())
    #expect(await changes.first("the last 3 rows") { $0.map(\.text) == ["t5", "t6", "t7"] } != nil)
    let atStart = feed.decodedRows

    transport.enqueue("ContainerThread", reply: Self.threadReply((1 ... 7).map { Self.threadNode($0, text: $0 == 1 || $0 == 6 ? "t\($0)x" : nil) }))
    try await thread.refresh()
    #expect(await changes.first("the edited row inside") { $0.map(\.text) == ["t5", "t6x", "t7"] } != nil)
    let afterEdits = feed.decodedRows

    transport.enqueue("CreateRecord", reply: #"{"data":{"createRecord":{"__typename":"Record","id":"m8","containerId":"c1","authorId":"a","text":"t8","createdAt":"2026-10-08T00:00:00.000Z","sequenceNumber":null}}}"#)
    _ = try await Record.create(containerId: "c1", authorId: "a", text: "t8")
    #expect(await changes.first("the new last row") { $0.map(\.text) == ["t6x", "t7", "t8"] } != nil)
    #expect(atStart == 3 && afterEdits - atStart == 1, "rows decoded for the window, then for an edit inside plus an edit before it")
    #expect(feed.decodedRows - afterEdits <= 2, "rows decoded for the new last row")
  }
}
