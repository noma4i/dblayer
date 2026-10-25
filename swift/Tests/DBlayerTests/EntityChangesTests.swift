import Foundation
import Testing
@testable import DBlayer

@MainActor
struct EntityChangesTests {
  func moment(_ id: String, _ text: String) -> Moment {
    Moment(id: id, uuid: id, text: text, media: .object([:]), createdAt: "2026-10-08T00:00:00.000Z")
  }

  func delivery(_ revision: Int64, _ rows: [Moment] = [], removed: [String] = []) -> EntityDecoding.Decoded<Moment> {
    EntityDecoding.Decoded(revision: revision, rows: rows.map { ($0, 1) }, removed: removed)
  }

  /// current, update, newest-only and removal
  @Test
  func streamDeliversCurrentThenNewest() async {
    let registry = Entities<Moment>(source: CountingSource())
    let entity = registry.entity("1")
    registry.apply(delivery(1, [moment("1", "A")]))
    let cursor = Cursor(entity.changes())
    #expect(await cursor.next("the current value")??.text == "A")
    registry.apply(delivery(2, [moment("1", "B")]))
    registry.apply(delivery(3, [moment("1", "C")]))
    #expect(await cursor.next("the newest value")??.text == "C")
    registry.apply(delivery(4, removed: ["1"]))
    let removed = await cursor.next("the removal")
    #expect(removed != nil && removed! == nil)
  }

  /// owner: one core observer for two readers, cancelling a reader that waits for its next value keeps the other, ending all releases entity and lease
  @Test
  func readersShareTheLeaseAndHoldTheEntity() async throws {
    let source = CountingSource()
    let events = Cursor(source.events)
    let registry = Entities<Moment>(source: source)
    let (progress, progressSink) = AsyncStream<String>.makeStream()
    let reports = Cursor(progress)
    weak var weakEntity: Entity<Moment>?
    var first: LiveStream<Moment?>?
    var second: LiveStream<Moment?>?
    do {
      let entity = registry.entity("1")
      weakEntity = entity
      first = entity.changes()
      second = entity.changes()
    }
    let cancelFirst = first!.cancel
    Task { @MainActor [first] in
      let cursor = Cursor(first!)
      _ = await cursor.next("the snapshot")
      progressSink.yield("snapshot read")
      progressSink.yield(await cursor.next("the value after cancel", timeout: .seconds(10)) == nil ? "ended" : "delivered")
    }
    first = nil
    try #require(weakEntity != nil)
    #expect(await events.next("the retain") == "retain 1")
    #expect(await reports.next("the reader taking its snapshot") == "snapshot read")
    cancelFirst()
    try #require(weakEntity != nil)
    registry.apply(delivery(1, [moment("1", "A")]))
    #expect(await reports.next("the cancelled reader ending", timeout: .seconds(10)) == "ended")

    do {
      let cursor = Cursor(second!)
      #expect(await cursor.next("the delivery")??.text == "A")
    }
    second = nil
    #expect(await events.next("the release") == "release 1")
    #expect(weakEntity == nil)
  }

  /// close ends the reader
  @Test
  func closedDatabaseEndsTheStream() async {
    let registry = Entities<Moment>(source: CountingSource())
    let entity = registry.entity("1")
    let cursor = Cursor(entity.changes())
    _ = await cursor.next("the first value")
    registry.close()
    #expect(await cursor.next("the end of the stream") == nil)
    let again = Cursor(entity.changes())
    #expect(await again.next("the end of a new stream") == nil)
  }

  /// a kept stream does not keep a closed entity
  @Test
  func closedDatabaseReleasesTheEntityOfAKeptStream() async {
    let registry = Entities<Moment>(source: CountingSource())
    weak var weakEntity: Entity<Moment>?
    let kept: LiveStream<Moment?>
    do {
      let entity = registry.entity("1")
      weakEntity = entity
      kept = entity.changes()
    }
    #expect(weakEntity != nil)
    registry.close()
    #expect(weakEntity == nil)
    _ = kept
  }
}
