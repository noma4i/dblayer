package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.ContainerShelf
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class NodeMembershipTest {
    @Test
    fun anEventNodeAddsAnExistingContainerToItsTypedShelf() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ContainerShelfQuery","document":"query ContainerShelfQuery { containers { id title shelves } }"},{"name":"ContainerNode","document":"query ContainerNode { node { ... on Container { id title shelves } } }"},{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }"}]""")
        transport.enqueue("ContainerShelfQuery", """{"data":{"containers":[]}}""")
        val shelf = Container.shelf(ContainerShelf.Primary)
        shelf.load()
        transport.enqueue("ContainerNode", """{"data":{"node":{"__typename":"Container","id":"c1","title":"same","shelves":["PRIMARY"]}}}""")
        Container.node("c1", force = true)
        assertTrue(shelf.live.current().isEmpty())
        val events = Container.events()
        val rows = async { shelf.live.flow.first { it.isNotEmpty() } }
        transport.enqueue("ContainerNode", """{"data":{"node":{"__typename":"Container","id":"c1","title":"same","shelves":["PRIMARY"]}}}""")
        transport.emit("RecordEvents", """{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"UPDATED","recordId":"c1"}]}}}""")
        assertEquals(listOf("c1"), rows.await().map { it.id })
        assertEquals(1, transport.calls.count { it.operationName == "ContainerShelfQuery" })
        assertEquals("c1", (events.first() as ModelEventItem.Event).event.id)
    }
}
