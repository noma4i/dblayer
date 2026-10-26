package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class ModelEventsTest {
    @Test
    fun declaredChangeEventsReachTheTypedFlowAndTheFlowEndsOnCancel() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]""")
        val first = async { Container.events.first() }
        eventually("the change subscription opens") { transport.calls.size == 1 }
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"DELETED","recordId":"c1"}]}}}""")
        val seen = (first.await() as ModelEventItem.Event).event
        assertEquals(Triple("CONTAINER", "DELETED", "c1"), Triple(seen.area, seen.kind, seen.id))
        assertNull(seen.before)
        assertNull(seen.after)
        assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })

        val collecting = launch { Container.events.collect { } }
        collecting.cancelAndJoin()
        assertEquals(true, collecting.isCancelled)
    }

    @Test
    fun aModelWithoutItsOwnChangesSourceReceivesTheEventsOfTheSourceAnotherModelDeclares() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]""")
        val first = async { Record.events.first() }
        eventually("the change subscription opens") { transport.calls.size == 1 }
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"RECORD","action":"CREATED","recordId":"r1"}]}}}""")
        val seen = (first.await() as ModelEventItem.Event).event
        assertEquals(Triple("RECORD", "CREATED", "r1"), Triple(seen.area, seen.kind, seen.id))
        assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })
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
        val first = async(start = CoroutineStart.UNDISPATCHED) { Container.events.first() }
        DBlayer.setListening(false)
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"DELETED","recordId":"c1"}]}}}""")
        assertEquals("c1", (first.await() as ModelEventItem.Event).event.id)
        assertEquals(listOf("RecordEvents"), transport.calls.map { it.operationName })
    }
}
