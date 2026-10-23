import Testing
@testable import DBlayer

@MainActor
struct EntityPhaseTests {
  func moment(_ id: String, _ text: String) -> Moment {
    Moment(id: id, uuid: id, text: text, media: .object([:]), createdAt: "2026-10-08T00:00:00.000Z")
  }

  @Test
  func phaseFollowsTheDeliveriesOfTheCore() {
    let registry = Entities<Moment>(source: CountingSource())
    let entity = registry.entity("1")
    #expect(entity.phase == .opening)
    registry.apply(EntityDecoding.Decoded(revision: 1, rows: [(moment("1", "A"), 1)], removed: []))
    #expect(entity.phase == .present)
    registry.apply(EntityDecoding.Decoded(revision: 2, rows: [], removed: [], failedIds: ["1": "bad row"]))
    #expect(entity.phase == .failed("bad row"))
    #expect(entity.value?.text == "A")
    registry.apply(EntityDecoding.Decoded(revision: 3, rows: [], removed: ["1"]))
    #expect(entity.phase == .absent)
    registry.apply(EntityDecoding.Decoded(revision: 4, rows: [], removed: [], failure: "read failed"))
    #expect(entity.phase == .failed("read failed"))
  }
}
