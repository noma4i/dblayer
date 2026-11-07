import Testing
@testable import DBlayer

struct ListEditTests {
  @Test
  func aRowThatEntersTheMiddleOfTheListLandsAtItsIndex() {
    var rows = ["n1", "n3"]
    rows.replay([.insert(1, "n2")])
    #expect(rows == ["n1", "n2", "n3"])
    rows.replay([.insert(0, "n0")])
    #expect(rows == ["n0", "n1", "n2", "n3"])
  }
}
