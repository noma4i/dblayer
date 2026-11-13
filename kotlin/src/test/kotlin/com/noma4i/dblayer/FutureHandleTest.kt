package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Test

class FutureHandleTest {
    private val document = "query ContainerMembers(\$containerId: ID!, \$first: Int, \$after: Cursor) { node(id: \$containerId) { ... on Container { membersCount members(first: \$first, after: \$after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

    private fun page(n: Int): String =
        """{"data":{"node":{"membersCount":99,"members":{"edges":[{"cursor":"a$n","node":{"id":"a$n","name":"Member"}}],"pageInfo":{"hasNextPage":false,"endCursor":"a$n"}}}}}"""

    /** bridge_prop_future_handle (P-BR-01): the cancel of a call and the answer of its request race from many threads, and every wait ends. */
    @Test
    fun bridge_prop_future_handle_cancelAndAnswerFromManyThreadsEndEveryWait() = bounded {
        val transport = MockTransport()
        openSigned(Config(null), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ContainerMembers")); put("document", JsonPrimitive(document)) }
        registerStandOperations(DBlayer.database, JsonArray(listOf(def)).toString())
        val holds = (0 until 8).map { transport.hold("ContainerMembers", page(it)) }
        val scope = CoroutineScope(Job() + Dispatchers.Default)
        try {
            val calls = (0 until 8).map { number -> scope.launch { User.containerMembers("c$number").loadAll() } }
            holds.forEach { hold -> awaitWithin("the request starts") { hold.entered() } }
            coroutineScope {
                for (number in 0 until 8) {
                    launch(Dispatchers.Default) { calls[number].cancel() }
                    launch(Dispatchers.Default) { holds[number].release() }
                }
            }
            calls.forEach { call -> awaitWithin("the wait of the call ends") { call.join() } }
            transport.enqueue("ContainerMembers", page(99))
            User.containerMembers("c99").loadAll()
            assertEquals(9, transport.calls.count { it.operationName == "ContainerMembers" })
        } finally {
            scope.cancel()
            closeForCleanup()
        }
    }
}
