import Testing
@testable import DBlayer

struct RelatedQueryTests {
  @Test func generatedRelationsCarryTheTargetTypeAndName() throws {
    let authors: RelatedQuery<User> = Moment.ids(["m1"]).related(Moment.Relations.author)
    let vibes: RelatedQuery<Vibe> = Moment.ids(["m1"]).related(Moment.Relations.vibes)
    #expect(authors.name == "author")
    #expect(vibes.name == "vibes")
    #expect(try authors.sourceJson(false) == (try Moment.ids(["m1"]).json))
  }
}
