package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CompletableDeferred
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private class OldSinkTransport(val base: MockTransport = MockTransport()) : Transport by base {
    val opened = CompletableDeferred<SubscriptionSink>()
    val closed = CompletableDeferred<String>()
    override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {
        base.subscribe(request, sink)
        opened.complete(sink)
    }
    override fun unsubscribe(token: String) {
        base.unsubscribe(token)
        closed.complete(token)
    }
}

@OptIn(DBlayerTestingApi::class)
class OldSinkTest {
    @Test
    fun oldSubscriptionSinkIsClosedBeforeTheNextAccount() = bounded {
        val transport = OldSinkTransport()
        try {
            DBlayer.close()
            transport.base.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"},{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
            val sink = object : ValueSink { override fun available() {} }
            val stream = DBlayer.database.subscribe(DBlayer.database.sessionEpoch(), StreamRequest.Query("""{"model":"Notice","relation":"recent","params":{}}"""), sink)
            val old = transport.opened.await()
            DBlayer.sessionReset()
            assertEquals("BulletinAdded", transport.closed.await())
            User.profile.load(AccountScope.current())
            old.event("""{"data":{"bulletinAdded":{"body":"old","occurredAt":"2026-10-18T00:00:00Z"}}}""")
            val rows = DBlayer.database.readQuery(DBlayer.database.sessionEpoch(), """{"model":"Notice","relation":"recent","params":{}}""")
            assertTrue(rows.isEmpty())
            DBlayer.database.cancelStream(stream)
        } finally {
            DBlayer.close()
        }
    }
}
