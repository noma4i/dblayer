package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.CopyOnWriteArrayList
import org.junit.Assert.assertEquals
import org.junit.Test

class SubscriptionSessionTest {
    @Test
    fun queryListenerReceivesOneTypedSessionFailureWithoutReads() = bounded {
        val mock = MockTransport()
        val subscription = java.util.concurrent.atomic.AtomicReference<SubscriptionSink>()
        val transport = object : Transport by mock {
            override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink): String {
                subscription.set(sink)
                return mock.subscribe(request, sink)
            }
        }
        val database = Database.open(Config(null, null, 8u), modelsJSON, transport)
        database.registerOperations("""[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]""")
        val failures = CopyOnWriteArrayList<DbException>()
        val listener = object : QueryListener {
            override fun changed(change: LiveChange) { change.error?.let { failures.add(it) } }
        }
        val live = database.liveQuery("""{"model":"Notice","relation":"recent","params":{}}""", listener)
        try {
            eventually("notice subscription", 2000) { subscription.get() != null }
            subscription.get().failed(DbException.Session(SessionCode.UNAUTHENTICATED, "session_unknown"))
            eventually("typed session failure", 2000) { failures.isNotEmpty() }
            assertEquals(1, failures.size)
            val failure = failures.single() as DbException.Session
            assertEquals(SessionCode.UNAUTHENTICATED, failure.code)
            assertEquals("session_unknown", failure.detail)
            assertEquals(listOf("BulletinAdded"), mock.calls.map { it.operationName })
        } finally { live.cancel() }
    }
}
