import Testing
@testable import DBlayer

struct MemberOrderTests {
  @Test
  func theOrderParamIsSentAsItsTextAndDefaultsToJoined() {
    #expect(Roster.containerMembers("c1").params == ["containerId": .string("c1"), "order": .string("JOINED")])
    #expect(Roster.containerMembers("c1", order: .ownerFirst).params == ["containerId": .string("c1"), "order": .string("OWNER_FIRST")])
    #expect(Roster.containerMembers("c1").params == Roster.containerMembers("c1", order: .joined).params)
    #expect(Roster.containerMembers("c1", order: .ownerFirst).params != Roster.containerMembers("c1", order: .joined).params)
    #expect(Roster.containerMembers("c1", order: .unknown("LATER")).params["order"] == .string("LATER"))
  }
}
