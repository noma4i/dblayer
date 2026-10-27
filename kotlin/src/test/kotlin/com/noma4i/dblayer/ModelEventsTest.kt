package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.async
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class ModelEventsTest {
    @Test
    fun theSameEventsFlowSupportsConcurrentCompletedAndCancelledCollections() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]""")
        val events = Container.events()
        val ready = java.util.concurrent.atomic.AtomicIntegerArray(4)
        fun emit(id: String) = transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"DELETED","recordId":"$id"}]}}}""")
        fun collect(reader: Int) = async {
            events.first { item ->
                if (item is ModelEventItem.Event && item.event.id == "probe") ready.set(reader, 1)
                item is ModelEventItem.Event && item.event.id == "target"
            }
        }
        val first = collect(0)
        val second = collect(1)
        eventually("both independent readers attach") { emit("probe"); ready.get(0) == 1 && ready.get(1) == 1 }
        emit("target")
        assertEquals("target", (first.await() as ModelEventItem.Event).event.id)
        assertEquals("target", (second.await() as ModelEventItem.Event).event.id)
        val cancelled = collect(2)
        eventually("a completed flow can be collected again") { emit("probe"); ready.get(2) == 1 }
        cancelled.cancelAndJoin()
        val restarted = collect(3)
        eventually("a cancelled flow can be collected again") { emit("probe"); ready.get(3) == 1 }
        emit("target")
        assertEquals("target", (restarted.await() as ModelEventItem.Event).event.id)
        assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })
    }

    private object RecordEvents : GraphQLOperation {
        override val operationName = "RecordEvents"
        override val document = "subscription RecordEvents { recordEvents { entries { topic action recordId } } }"
    }

    @OptIn(DBlayerTestingApi::class)
    @Test
    fun openTestingHoldsSubscriptionsUntilRegistrationCompletes() = bounded {
        DBlayer.close()
        try {
            val transport = MockTransport()
            DBlayer.openTesting(transport, modelsJSON, operations = listOf(RecordEvents), listening = false)
            DBlayer.resume()
            assertEquals(emptyList<String>(), transport.calls.map { it.operationName })
            DBlayer.setListening(true)
            eventually("registration complete starts the subscription") { transport.calls.size == 1 }
            DBlayer.setListening(true)
            DBlayer.resume()
            assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })
        } finally {
            DBlayer.close()
        }
    }

    @Test
    fun declaredChangeEventsReachTheTypedFlowAndTheFlowEndsOnCancel() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]""")
        val events = Container.events()
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"DELETED","recordId":"c1"}]}}}""")
        val seen = (events.first() as ModelEventItem.Event).event
        assertEquals(Triple("CONTAINER", "DELETED", "c1"), Triple(seen.area, seen.kind, seen.id))
        assertNull(seen.before)
        assertNull(seen.after)
        assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })

        val nextEvents = Container.events()
        val collecting = launch(start = CoroutineStart.UNDISPATCHED) { nextEvents.collect { } }
        collecting.cancelAndJoin()
        assertEquals(true, collecting.isCancelled)
    }

    @Test
    fun aModelWithoutItsOwnChangesSourceReceivesTheEventsOfTheSourceAnotherModelDeclares() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]""")
        val events = Record.events()
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"RECORD","action":"CREATED","recordId":"r1"}]}}}""")
        val seen = (events.first() as ModelEventItem.Event).event
        assertEquals(Triple("RECORD", "CREATED", "r1"), Triple(seen.area, seen.kind, seen.id))
        assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })
    }

    @Test
    fun aStaleModelEndsTheEventsWithTheDecodeFailure() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"},{"name":"ContainerNode","document":"query ContainerNode(${'$'}id: ID!) { node(id: ${'$'}id) { id title shelves } }","rootPath":"node"}]""")
        val stale = object : ModelDecoder<Container> by Container { override val layout = "stale" }
        val events = stale.openEvents()
        transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"received","shelves":["PRIMARY"]}}}""")
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"UPDATED","recordId":"c1"}]}}}""")
        val items = events.toList()
        assertEquals(1, items.size)
        assertEquals(true, (items.single() as ModelEventItem.Failed).error is DbException.Invalid)
    }

    @Test
    fun aDeniedListeningOpensNoSubscriptionFromAStoredCredentialUntilTheAppAllowsIt() = bounded {
        val transport = MockTransport()
        val operations = """[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]"""
        reopen(Config(null, null, 32u), modelsJSON, transport, operations = operations, listening = false)
        DBlayer.resume()
        assertEquals(emptyList<String>(), transport.calls.map { it.operationName })
        DBlayer.setListening(true)
        eventually("the subscriptions start") { transport.calls.size == 1 }
        DBlayer.setListening(true)
        DBlayer.resume()
        assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })
        val events = Container.events()
        DBlayer.setListening(false)
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"DELETED","recordId":"c1"}]}}}""")
        assertEquals("c1", (events.first() as ModelEventItem.Event).event.id)
        assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })
    }
}
