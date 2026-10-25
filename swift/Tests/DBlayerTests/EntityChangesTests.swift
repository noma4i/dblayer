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
    var iterator = entity.changes().makeAsyncIterator()
    #expect(await iterator.next()??.text == "A")
    registry.apply(delivery(2, [moment("1", "B")]))
    registry.apply(delivery(3, [moment("1", "C")]))
    #expect(await iterator.next()??.text == "C")
    registry.apply(delivery(4, removed: ["1"]))
    let removed = await iterator.next()
    #expect(removed != nil && removed! == nil)
  }

  /// owner: one core observer for two readers, cancelling one keeps the other, ending all releases entity and lease
  @Test
  func readersShareTheLeaseAndHoldTheEntity() async throws {
    let source = CountingSource()
    var events = source.events.makeAsyncIterator()
    let registry = Entities<Moment>(source: source)
    weak var weakEntity: Entity<Moment>?
    var second: LiveStream<Moment?>?
    let reader: Task<Void, Never>
    do {
      let entity = registry.entity("1")
      weakEntity = entity
      let first = entity.changes()
      second = entity.changes()
      reader = Task { @MainActor in for await _ in first {} }
    }
    try #require(weakEntity != nil)
    #expect(await events.next() == "retain 1")
    reader.cancel()
    await reader.value
    try #require(weakEntity != nil)

    registry.apply(delivery(1, [moment("1", "A")]))
    do {
      var iterator = second!.makeAsyncIterator()
      #expect(await iterator.next()??.text == "A")
    }
    second = nil
    #expect(await events.next() == "release 1")
    #expect(weakEntity == nil)
  }

  /// close ends the reader
  @Test
  func closedDatabaseEndsTheStream() async {
    let registry = Entities<Moment>(source: CountingSource())
    let entity = registry.entity("1")
    var iterator = entity.changes().makeAsyncIterator()
    _ = await iterator.next()
    registry.close()
    #expect(await iterator.next() == nil)
    var again = entity.changes().makeAsyncIterator()
    #expect(await again.next() == nil)
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
