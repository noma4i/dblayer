package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CompletableDeferred
import org.junit.Assert.assertEquals
import org.junit.Test

class SubscriptionSessionTest {
    private enum class Order(val failsBeforeOpened: Boolean) {
        BeforeOpenedAndReady(true),
        AfterOpenedAndReady(false),
    }

    @Test
    fun queryListenerReceivesOneTypedSessionFailureWithoutReads() = bounded {
        for (order in Order.entries) deliversOneTypedSessionFailure(order)
    }

    private suspend fun deliversOneTypedSessionFailure(order: Order) {
        val events = CopyOnWriteArrayList<String>()
        val mock = MockTransport()
        val sinkHandedOut = CompletableDeferred<SubscriptionSink>()
        val failureSent = CompletableDeferred<Unit>()
        val openedAndReadySent = CompletableDeferred<Unit>()
        val transport = object : Transport by mock {
            override suspend fun subscribe(request: TransportRequest, sink: SubscriptionSink) {
                events.add("subscribe entered")
                sinkHandedOut.complete(sink)
                if (order.failsBeforeOpened) failureSent.await()
                mock.subscribe(request, sink)
                events.add("opened and ready sent")
                openedAndReadySent.complete(Unit)
            }
        }
        val database = openDatabase(Config(null, null, 8u), modelsJSON, transport)
        database.registerOperations("""[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]""")
        val failures = CopyOnWriteArrayList<DbException>()
        val delivered = CompletableDeferred<DbException>()
        val listener = object : QueryListener {
            override fun changed(change: LiveChange) { change.error?.let { failures.add(it); events.add("listener received error"); delivered.complete(it) } }
        }
        val live = database.liveQuery("""{"model":"Notice","relation":"recent","params":{}}""", listener)
        suspend fun <T> step(what: String, block: suspend () -> T): T = try {
            awaitWithin(what, block)
        } catch (error: AssertionError) {
            throw AssertionError("case $order, events reached: $events\n${error.message}", error.cause)
        }
        try {
            val sink = step("the transport subscribe call") { sinkHandedOut.await() }
            if (!order.failsBeforeOpened) step("opened and ready sent") { openedAndReadySent.await() }
            events.add("failed sent")
            sink.failed(DbException.Session(SessionCode.UNAUTHENTICATED, "session_unknown"))
            failureSent.complete(Unit)
            step("the typed session failure for the query listener") { delivered.await() }
            if (order.failsBeforeOpened) step("opened and ready sent after the failure") { openedAndReadySent.await() }
            assertEquals("case $order", 1, failures.size)
            val failure = failures.single() as DbException.Session
            assertEquals("case $order", SessionCode.UNAUTHENTICATED, failure.code)
            assertEquals("case $order", "session_unknown", failure.detail)
            assertEquals("case $order", listOf("BulletinAdded"), mock.calls.map { it.operationName })
        } finally { live.cancel(); database.close() }
    }
}
