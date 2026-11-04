package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertNull
import org.junit.Test

class LoadAllCancellationTest {
    private val document = "query ContainerMembers(\$containerId: ID!, \$first: Int, \$after: Cursor) { node(id: \$containerId) { ... on Container { membersCount members(first: \$first, after: \$after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

    private fun page(n: Int): String =
        """{"data":{"node":{"membersCount":99,"members":{"edges":[{"cursor":"a$n","node":{"id":"a$n","name":"Member"}}],"pageInfo":{"hasNextPage":true,"endCursor":"a$n"}}}}}"""

    /** The order of the app: a long page chain read by loadAll in a scope, the scope cancelled while a page is on its way. */
    @Test
    fun cancellingTheCallerOfLoadAllStopsTheTransportCalls() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 64u), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ContainerMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        repeat(5) { transport.enqueue("ContainerMembers", page(it)) }
        val sixth = transport.hold("ContainerMembers", page(5))
        val seventh = transport.hold("ContainerMembers", page(6))

        val scope = CoroutineScope(Job() + Dispatchers.Default)
        val running = scope.launch { User.containerMembers("c1").loadAll() }
        awaitWithin("the sixth page request") { sixth.entered() }
        scope.coroutineContext[Job]!!.cancelAndJoin()
        running.join()
        sixth.release()

        val asked = withTimeoutOrNull(1_500) { seventh.entered(); true }
        assertNull("the chain asked for the seventh page after its caller was cancelled", asked)
    }
}
