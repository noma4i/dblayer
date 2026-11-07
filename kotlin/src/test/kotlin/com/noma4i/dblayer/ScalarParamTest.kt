package com.noma4i.dblayer

import com.noma4i.dblayer.models.ContainerMemberOrder
import com.noma4i.dblayer.models.ContainerShelfKind
import com.noma4i.dblayer.models.Inbox
import com.noma4i.dblayer.models.Roster
import com.noma4i.dblayer.models.Tail
import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

class ScalarParamTest {
    private val document = "query tailSince(\$containerId: ID!, \$afterSequence: Int, \$pinned: Boolean, \$first: Int, \$after: String) { tailSince(containerId: \$containerId, afterSequence: \$afterSequence, pinned: \$pinned, first: \$first, after: \$after) { edges { cursor node { id text } } pageInfo { hasNextPage endCursor } } }"

    @Test
    fun aScopeParamIsSentAsItsJsonValue() {
        val cases = listOf(
            "an int and a bool" to (Tail.since("c1", afterSequence = 120L, pinned = true).json to """{"model":"Tail","relation":"since","params":{"containerId":"c1","afterSequence":120,"pinned":true}}"""),
            "another int" to (Tail.since("c1", afterSequence = 121L, pinned = true).json to """{"model":"Tail","relation":"since","params":{"containerId":"c1","afterSequence":121,"pinned":true}}"""),
            "a schema enum" to (Inbox.shelf(ContainerShelfKind.Primary).json to """{"model":"Inbox","relation":"shelf","params":{"list":"PRIMARY"}}"""),
            "another schema enum value" to (Inbox.shelf(ContainerShelfKind.Secondary).json to """{"model":"Inbox","relation":"shelf","params":{"list":"SECONDARY"}}"""),
            "a schema enum value the build lacks" to (Inbox.shelf(ContainerShelfKind.Unknown("LATER")).json to """{"model":"Inbox","relation":"shelf","params":{"list":"LATER"}}"""),
            "the default order" to (Roster.containerMembers("c1").json to """{"model":"Roster","relation":"container_members","params":{"containerId":"c1","order":"JOINED"}}"""),
            "an order of the schema" to (Roster.containerMembers("c1", order = ContainerMemberOrder.OwnerFirst).json to """{"model":"Roster","relation":"container_members","params":{"containerId":"c1","order":"OWNER_FIRST"}}"""),
        )
        for ((name, pair) in cases) assertEquals(name, pair.second, pair.first)
    }
    @Test
    fun aTypedScopeParamReachesTheServerAsANumber() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"tailSince","document":"$document"}]""")
        transport.enqueue("tailSince", """{"data":{"tailSince":{"edges":[{"cursor":"c1","node":{"id":"t1","text":"a"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c1"}}}}""")
        Tail.since("c1", afterSequence = 120L, pinned = false).refresh()
        assertEquals(parseJson("""{"containerId":"c1","afterSequence":120,"pinned":false,"first":50}"""), parseJson(transport.calls.first { it.operationName == "tailSince" }.variablesJson))
    }

}
