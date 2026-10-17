import Testing
@testable import DBlayer

struct MemberOrderTests {
  @Test
  func theOrderParamIsSentAsItsTextAndDefaultsToJoined() {
    #expect(Roster.chatMembers("c1").params == ["chatId": .string("c1"), "order": .string("JOINED")])
    #expect(Roster.chatMembers("c1", order: .ownerFirst).params == ["chatId": .string("c1"), "order": .string("OWNER_FIRST")])
    #expect(Roster.chatMembers("c1").params == Roster.chatMembers("c1", order: .joined).params)
    #expect(Roster.chatMembers("c1", order: .ownerFirst).params != Roster.chatMembers("c1", order: .joined).params)
    #expect(Roster.chatMembers("c1", order: .unknown("LATER")).params["order"] == .string("LATER"))
  }
}
