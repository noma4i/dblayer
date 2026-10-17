package com.noma4i.dblayer

import com.noma4i.dblayer.models.ChatMemberOrder
import com.noma4i.dblayer.models.Roster
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test

class MemberOrderTest {
    @Test
    fun theOrderParamIsSentAsItsTextAndDefaultsToJoined() {
        assertEquals("""{"model":"Roster","relation":"chat_members","params":{"chatId":"c1","order":"JOINED"}}""", Roster.chatMembers("c1").json)
        assertEquals("""{"model":"Roster","relation":"chat_members","params":{"chatId":"c1","order":"OWNER_FIRST"}}""", Roster.chatMembers("c1", order = ChatMemberOrder.OwnerFirst).json)
        assertEquals(Roster.chatMembers("c1").json, Roster.chatMembers("c1", ChatMemberOrder.Joined).json)
        assertNotEquals(Roster.chatMembers("c1", ChatMemberOrder.OwnerFirst).json, Roster.chatMembers("c1").json)
    }
}
