package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger

private class PageTransport(val mock: MockTransport = MockTransport(), private val cancelAt: Int = 6) : Transport by mock {
    private val requests = AtomicInteger()
    val sixthEnded = CompletableDeferred<Boolean>()

    override suspend fun query(request: TransportRequest): String {
        val number = if (request.operationName == "ContainerMembers") requests.incrementAndGet() else 0
        try {
            return mock.query(request)
        } finally {
            if (number == cancelAt) sixthEnded.complete(!currentCoroutineContext().isActive)
        }
    }
}

class LoadAllCancellationTest {
    private val document = "query ContainerMembers(\$containerId: ID!, \$first: Int, \$after: Cursor) { node(id: \$containerId) { ... on Container { membersCount members(first: \$first, after: \$after) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }"

    private fun page(n: Int): String =
        """{"data":{"node":{"membersCount":99,"members":{"edges":[{"cursor":"a$n","node":{"id":"a$n","name":"Member"}}],"pageInfo":{"hasNextPage":true,"endCursor":"a$n"}}}}}"""

    @Test
    fun cancellingTheLastLoadInterestCancelsTheRequest() = bounded {
        val transport = PageTransport(cancelAt = 1)
        reopen(Config(null, null), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ContainerMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        val held = transport.mock.hold("ContainerMembers", page(0))
        val request = User.containerMembers("c1").beginLoad()
        try {
            awaitWithin("the request starts") { held.entered() }
            request.cancel()
            assertTrue("the request outlived its last interest", awaitWithin("the transport cancels") { transport.sixthEnded.await() })
            assertEquals(1, transport.mock.calls.count { it.operationName == "ContainerMembers" })
        } finally {
            request.cancel()
            held.release()
            closeForCleanup()
        }
    }

    /** The order of the app: a long page chain read by loadAll in a scope, the scope cancelled while a page is on its way. */
    @Test
    fun cancellingTheCallerOfLoadAllStopsTheTransportCalls() = bounded {
        val transport = PageTransport()
        reopen(Config(null, null), modelsJSON, transport)
        val def = buildJsonObject { put("name", JsonPrimitive("ContainerMembers")); put("document", JsonPrimitive(document)) }
        DBlayer.database.registerOperations(JsonArray(listOf(def)).toString())
        repeat(5) { transport.mock.enqueue("ContainerMembers", page(it)) }
        val sixth = transport.mock.hold("ContainerMembers", page(5))

        val scope = CoroutineScope(Job() + Dispatchers.Default)
        try {
            val running = scope.launch { User.containerMembers("c1").loadAll() }
            awaitWithin("the sixth page request") { sixth.entered() }
            scope.coroutineContext[Job]!!.cancelAndJoin()
            running.join()
            assertTrue("the sixth request was not cancelled", awaitWithin("the sixth request to end") { transport.sixthEnded.await() })
            assertEquals(6, transport.mock.calls.count { it.operationName == "ContainerMembers" })
        } finally {
            sixth.release()
            scope.cancel()
            closeForCleanup()
        }
    }
}
