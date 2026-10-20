package com.noma4i.dblayer

import com.noma4i.dblayer.models.Operations
import com.noma4i.dblayer.models.Tail
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test

class ScalarParamTest {
    private val document = "query tailSince(\$chatId: ID!, \$afterSequence: Int, \$pinned: Boolean, \$first: Int, \$after: String) { tailSince(chatId: \$chatId, afterSequence: \$afterSequence, pinned: \$pinned, first: \$first, after: \$after) { edges { cursor node { id text } } pageInfo { hasNextPage endCursor } } }"

    @Test
    fun aScopeParamOfIntAndBoolIsSentAsItsJsonValue() {
        val query = Tail.since("c1", afterSequence = 120L, pinned = true)
        assertEquals("""{"model":"Tail","relation":"since","params":{"chatId":"c1","afterSequence":120,"pinned":true}}""", query.json)
        assertNotEquals(query.json, Tail.since("c1", afterSequence = 121L, pinned = true).json)
    }

    @Test
    fun aTypedScopeParamReachesTheServerAsANumber() = bounded {
        val transport = MockTransport()
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"tailSince","document":"$document"}]""")
        transport.enqueue("tailSince", """{"data":{"tailSince":{"edges":[{"cursor":"c1","node":{"id":"t1","text":"a"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c1"}}}}""")
        Tail.since("c1", afterSequence = 120L, pinned = false).refresh()
        assertEquals(parseJson("""{"chatId":"c1","afterSequence":120,"pinned":false,"first":50}"""), parseJson(transport.calls.first { it.operationName == "tailSince" }.variablesJson))
    }

    @Test
    fun anAggregateOperationReturnsItsFieldsAsAValue() = bounded {
        val transport = MockTransport()
        transport.enqueue("chatMedia", """{"data":{"chatMedia":{"visualCount":3,"audioCount":1,"giftCount":0,"latestSequence":120}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"chatMedia","document":"query chatMedia(\${'$'}chatId: ID!, \${'$'}upToSequence: Int) { chatMedia(chatId: \${'$'}chatId, upToSequence: \${'$'}upToSequence) { visualCount audioCount giftCount latestSequence } }"}]""")
        val result = withTimeout(5000) { Operations.chatMediaLoad(chatId = "c1", upToSequence = 120) }
        assertEquals(3L, result.visualCount)
        assertEquals(120L, result.latestSequence)
        assertEquals(parseJson("""{"chatId":"c1","upToSequence":120}"""), parseJson(transport.calls.first { it.operationName == "chatMedia" }.variablesJson))
    }
}
