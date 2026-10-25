package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CompletableDeferred
import org.junit.Assert.assertEquals
import org.junit.Test

class SubscriptionSessionTest {
    @Test
    fun queryListenerReceivesOneTypedSessionFailureWithoutReads() = bounded {
        val mock = MockTransport()
        val subscription = CompletableDeferred<SubscriptionSink>()
        val transport = object : Transport by mock {
            override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {
                subscription.complete(sink)
                mock.subscribe(request, sink)
            }
        }
        val database = openDatabase(Config(null, null, 8u), modelsJSON, transport)
        database.registerOperations("""[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]""")
        val failures = CopyOnWriteArrayList<DbException>()
        val delivered = CompletableDeferred<DbException>()
        val listener = object : QueryListener {
            override fun changed(change: LiveChange) { change.error?.let { failures.add(it); delivered.complete(it) } }
        }
        val live = database.liveQuery("""{"model":"Notice","relation":"recent","params":{}}""", listener)
        try {
            awaitWithin("the transport subscribe call") { subscription.await() }.failed(DbException.Session(SessionCode.UNAUTHENTICATED, "session_unknown"))
            awaitWithin("the typed session failure for the query listener") { delivered.await() }
            assertEquals(1, failures.size)
            val failure = failures.single() as DbException.Session
            assertEquals(SessionCode.UNAUTHENTICATED, failure.code)
            assertEquals("session_unknown", failure.detail)
            assertEquals(listOf("BulletinAdded"), mock.calls.map { it.operationName })
        } finally { live.cancel() }
    }
}
