package com.noma4i.dblayer

import com.noma4i.dblayer.models.ContainerMemberOrder
import com.noma4i.dblayer.models.Roster
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test

class MemberOrderTest {
    @Test
    fun theOrderParamIsSentAsItsTextAndDefaultsToJoined() {
        assertEquals("""{"model":"Roster","relation":"container_members","params":{"containerId":"c1","order":"JOINED"}}""", Roster.containerMembers("c1").json)
        assertEquals("""{"model":"Roster","relation":"container_members","params":{"containerId":"c1","order":"OWNER_FIRST"}}""", Roster.containerMembers("c1", order = ContainerMemberOrder.OwnerFirst).json)
        assertEquals(Roster.containerMembers("c1").json, Roster.containerMembers("c1", ContainerMemberOrder.Joined).json)
        assertNotEquals(Roster.containerMembers("c1", ContainerMemberOrder.OwnerFirst).json, Roster.containerMembers("c1").json)
    }
}
