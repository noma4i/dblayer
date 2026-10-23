package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.job
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class SubscriptionTokenAfterCancelTest {
    private val operations = """[{"name":"documentBySlug","document":"query documentBySlug(${'$'}slug: String!, ${'$'}source: String!) { documentBySlug(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated(${'$'}slug: String!, ${'$'}source: String!) { documentUpdated(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentUpdated"}]"""

    @OptIn(InternalCoroutinesApi::class)
    @Test
    fun aTokenReturnedAfterTheCoreDroppedTheCallIsHandedBack() = bounded {
        val mock = MockTransport()
        mock.enqueue("documentBySlug", """{"data":{"documentBySlug":{"id":"d1","slug":"a"}}}""")
        val issued = CopyOnWriteArrayList<String>()
        val unsubscribed = CopyOnWriteArrayList<String>()
        lateinit var database: Database
        val transport = object : Transport by mock {
            override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink): String {
                val token = mock.subscribe(request, sink)
                issued.add(token)
                database.close()
                val dropped = CompletableDeferred<Unit>()
                currentCoroutineContext().job.invokeOnCompletion(onCancelling = true, invokeImmediately = true) { dropped.complete(Unit) }
                withContext(NonCancellable) { withTimeout(HANG_GUARD_MILLIS) { dropped.await() } }
                return token
            }
            override fun unsubscribe(token: String) { unsubscribed.add(token); mock.unsubscribe(token) }
        }
        database = openDatabase(Config(null, null, 8u), modelsJSON, transport)
        database.registerOperations(operations)
        val listener = object : QueryListener { override fun changed(change: LiveChange) {} }
        database.liveQuery("""{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}""", listener)
        eventually("the call returns its token") { issued.size == 1 }
        eventually("the token is handed back") { unsubscribed.size == 1 }
        assertEquals(issued.toList(), unsubscribed.toList())
    }
}
