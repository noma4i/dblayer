package com.noma4i.dblayer

import com.noma4i.dblayer.models.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.serialization.json.*
import org.junit.Assert.*
import org.junit.Test

class EntityRegistryTest {
    @Test
    fun entityInitialLocal() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        val row = Moment.create(text = "local")
        val entered = CountDownLatch(1)
        val allow = CountDownLatch(1)
        val decoder = object : ModelDecoder<Moment> by Moment {
            override fun decode(row: WireRow): Moment {
                entered.countDown()
                check(allow.await(5, TimeUnit.SECONDS)) { "entity decode gate timed out" }
                return Moment.decode(row)
            }
        }
        val entity = Entities("Moment", decoder).entity(row.id)
        val waiting = async(start = CoroutineStart.UNDISPATCHED) { entity.waitForInitialLoad() }
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            assertFalse(waiting.isCompleted)
            assertEquals(EntityPhase.Opening, entity.snapshot.value.phase)
        } finally { allow.countDown() }
        waiting.await()
        assertEquals("local", entity.snapshot.value.value?.text)
        val absent = Moment.entity("missing")
        absent.waitForInitialLoad()
        assertEquals(EntityPhase.Absent, absent.snapshot.value.phase)
        assertNull(absent.snapshot.value.value)
        assertTrue(transport.calls.isEmpty())
    }

    @Test
    fun entityIfMissingPlatforms() = bounded {
        val transport = MockTransport()
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"ContainerShelfQuery","document":"query ContainerShelfQuery { containers { id title shelves } }"},{"name":"ContainerNode","document":"query ContainerNode { node { ... on Container { id title shelves } } }"}]""")
        transport.enqueue("ContainerShelfQuery", """{"data":{"containers":[{"id":"from-list","title":"local","shelves":["PRIMARY"]}]}}""")
        Container.shelf(ContainerShelf.Primary).load()
        val present = Container.entity("from-list", EntityFetch.IF_MISSING)
        present.waitForInitialLoad()
        eventually("the presence read settles") { present.read.value == EntityRead.Loaded }
        assertEquals("local", present.value.value?.title)
        assertEquals(0, transport.calls.count { it.operationName == "ContainerNode" })
        transport.enqueue("ContainerNode", """{"data":{"node":{"__typename":"Container","id":"missing","title":"node","shelves":["PRIMARY"]}}}""")
        val absent = Container.entity("missing", EntityFetch.IF_MISSING)
        absent.waitForInitialLoad()
        assertEquals("node", absent.snapshot.value.value?.title)
        assertEquals(1, transport.calls.count { it.operationName == "ContainerNode" })
    }

    @Test
    fun oneValuePerIdAndOnlyTheChangedRowDecodes() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val a = Moment.create(text = "A")
        val b = Moment.create(text = "B")
        val first = Moment.entity(a.id)
        val second = Moment.entity(b.id)
        first.waitForInitialLoad()
        second.waitForInitialLoad()
        assertSame(first, Moment.entity(a.id))
        val kept = second.snapshot.value
        a.update(text = GraphQLNullable.Value("new"))
        eventually("the changed row arrives") { first.value.value?.text == "new" }
        assertSame(kept, second.snapshot.value)
    }

    @Test
    fun releasingTheOwnerReturnsTheSubscriptionCensusToItsBase() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, null)
        val row = Moment.create(text = "held")
        val base = DBlayer.subscriptionCensus().subscriptions
        var owner: Entity<Moment>? = Moment.entity(row.id)
        owner!!.waitForInitialLoad()
        val weak = java.lang.ref.WeakReference(owner)
        owner = null
        eventually("the lease releases") { System.gc(); weak.get() == null && DBlayer.subscriptionCensus().subscriptions == base }
        val again = Moment.entity(row.id)
        again.waitForInitialLoad()
        assertEquals("held", again.value.value?.text)
    }
}
