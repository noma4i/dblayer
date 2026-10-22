package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

class SubscriptionInFlightTest {
    private val operations = """[{"name":"documentBySlug","document":"query documentBySlug(${'$'}slug: String!, ${'$'}source: String!) { documentBySlug(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated(${'$'}slug: String!, ${'$'}source: String!) { documentUpdated(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentUpdated"}]"""

    @Test
    fun closingWhileSubscribeIsInFlightCancelsTheForeignCoroutineAndIssuesNoToken() = bounded {
        val mock = MockTransport()
        mock.enqueue("documentBySlug", """{"data":{"documentBySlug":{"id":"d1","slug":"a"}}}""")
        val entered = CompletableDeferred<Unit>()
        val gate = CompletableDeferred<Unit>()
        val finished = CompletableDeferred<Result<String>>()
        val unsubscribed = CopyOnWriteArrayList<String>()
        val transport = object : Transport by mock {
            override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink): String {
                entered.complete(Unit)
                try {
                    gate.await()
                    val token = mock.subscribe(request, sink)
                    finished.complete(Result.success(token))
                    return token
                } catch (error: Throwable) {
                    finished.complete(Result.failure(error))
                    throw error
                }
            }
            override fun unsubscribe(token: String) { unsubscribed.add(token); mock.unsubscribe(token) }
        }
        val database = openDatabase(Config(null, null, 8u), modelsJSON, transport)
        database.registerOperations(operations)
        val listener = object : QueryListener { override fun changed(change: LiveChange) {} }
        val live = database.liveQuery("""{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}""", listener)
        withTimeout(HANG_GUARD_MILLIS) { entered.await() }
        live.cancel()
        database.close()
        val outcome = withTimeout(HANG_GUARD_MILLIS) { finished.await() }
        assertEquals(true, outcome.exceptionOrNull() is kotlinx.coroutines.CancellationException)
        assertEquals(emptyList<String>(), unsubscribed.toList())
    }
}
