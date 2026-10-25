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

  func settle() async { for _ in 0 ..< 5 { await Task.yield() } }

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

  /// owner: one core observer for two readers, cancel of one keeps the other, drop releases the entity
  @Test
  func readersShareTheLeaseAndHoldTheEntity() async {
    let source = CountingSource()
    let registry = Entities<Moment>(source: source)
    weak var weakEntity: Entity<Moment>?
    var second: LiveStream<Moment?>?
    do {
      let entity = registry.entity("1")
      weakEntity = entity
      let first = entity.changes()
      second = entity.changes()
      await settle()
      #expect(source.retained == ["1"] && source.released.isEmpty)
      registry.apply(delivery(1, [moment("1", "A")]))
      var iterator = first.makeAsyncIterator()
      #expect(await iterator.next()??.text == "A")
    }
    await settle()
    #expect(weakEntity != nil)
    registry.apply(delivery(2, [moment("1", "B")]))
    do {
      var iterator = second!.makeAsyncIterator()
      #expect(await iterator.next()??.text == "B")
    }
    second = nil
    await settle()
    #expect(weakEntity == nil)
    await settle()
    #expect(source.released == ["1"])
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
    await settle()
    #expect(weakEntity == nil)
    _ = kept
  }
}
