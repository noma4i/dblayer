package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.launch
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.job
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test

@OptIn(DelicateCoroutinesApi::class)
class SubscriptionTokenAfterCancelTest {
    private val operations = """[{"name":"documentBySlug","document":"query documentBySlug(${'$'}slug: String!, ${'$'}source: String!) { documentBySlug(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated(${'$'}slug: String!, ${'$'}source: String!) { documentUpdated(slug: ${'$'}slug, source: ${'$'}source) { id slug } }","rootPath":"documentUpdated"}]"""

    @OptIn(InternalCoroutinesApi::class)
    private suspend fun untilTheCoreDropsTheCall() {
        val dropped = CompletableDeferred<Unit>()
        currentCoroutineContext().job.invokeOnCompletion(onCancelling = true, invokeImmediately = true) { dropped.complete(Unit) }
        withContext(NonCancellable) { withTimeout(HANG_GUARD_MILLIS) { dropped.await() } }
    }

    private suspend fun runWith(announce: suspend (SubscriptionSink, String, () -> Unit) -> Unit): Pair<List<String>, List<String>> {
        val mock = MockTransport()
        mock.enqueue("documentBySlug", """{"data":{"documentBySlug":{"id":"d1","slug":"a"}}}""")
        val issued = CopyOnWriteArrayList<String>()
        val unsubscribed = CopyOnWriteArrayList<String>()
        lateinit var database: Database
        val transport = object : Transport by mock {
            override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {
                val token = "token-${request.operationName}"
                issued.add(token)
                announce(sink, token) { kotlinx.coroutines.GlobalScope.launch { database.closeDatabase() } }
            }
            override fun unsubscribe(token: String) { unsubscribed.add(token); mock.unsubscribe(token) }
        }
        database = openDatabase(Config(null, null, 8u), modelsJSON, transport)
        database.registerOperations(operations)
        val listener = object : ValueSink { override fun available() {} }
        database.subscribe(database.sessionEpoch(), StreamRequest.Query("""{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}"""), listener)
        eventually("the call issues its token") { issued.size == 1 }
        eventually("the token is handed back") { unsubscribed.size == 1 }
        return issued.toList() to unsubscribed.toList()
    }

    @Test
    fun aTokenOpenedBeforeTheCoreDroppedTheCallIsHandedBack() = bounded {
        val (issued, unsubscribed) = runWith { sink, token, close ->
            sink.opened(token)
            close()
            untilTheCoreDropsTheCall()
        }
        assertEquals(issued, unsubscribed)
    }

    @Test
    fun aTokenOpenedAfterTheCoreDroppedTheCallIsHandedBack() = bounded {
        val (issued, unsubscribed) = runWith { sink, token, close ->
            close()
            untilTheCoreDropsTheCall()
            sink.opened(token)
        }
        assertEquals(issued, unsubscribed)
    }
}
