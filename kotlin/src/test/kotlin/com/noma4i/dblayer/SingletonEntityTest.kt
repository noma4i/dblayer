package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class SingletonEntityTest {
    private val transport = MockTransport()

    private suspend fun open() {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
    }

    /** The server answers the profile request with this user. */
    private suspend fun login(id: String, name: String) {
        transport.enqueue("UserProfile", reply = """{"data":{"me":null}}""")
        User.profile.load()
        transport.enqueue("UserProfile", reply = """{"data":{"me":{"__typename":"User","id":"$id","name":"$name"}}}""")
        User.profile.refresh()
    }

    private suspend fun loaded(entity: Entity<User>) = withTimeout(5000) { entity.value.first { it != null } }

    private val registry get() = EntityRegistries.of("User", User)

    @Test
    fun singletonRefreshStoresBeforeAnyReaderExists() = bounded {
        open()
        assertEquals(0uL, DBlayer.subscriptionCensus().observedQueries)
        login("1", "Ann")
        User.profile.warm()
        val rows = User.profile.cached()!!.value
        assertEquals(listOf("1"), rows.map { it.id })
        assertEquals("Ann", rows.firstOrNull()?.name)
        assertEquals(0uL, DBlayer.subscriptionCensus().observedQueries)
        assertEquals("Ann", loaded(User.entity())?.name)
    }

    /** REL-01 for the singleton and REL-08: 10 readers share 1 object, 1 value and 1 core subscription. */
    @Test
    fun tenReadersCostOneSubscriptionAndSeeOneValue() = bounded {
        open()
        val readers = List(10) { User.entity() }
        val feed = registry.currentSingleFeed
        login("1", "Ann")
        loaded(readers[0])
        assertEquals(1, readers.map { System.identityHashCode(it) }.toSet().size)
        assertTrue(feed != null && feed === registry.currentSingleFeed)
        assertTrue(readers.all { it.value.value?.name == "Ann" && it.id == readers[0].value.value?.id })
        assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
    }

    /** The value is null before the first load and a collector is told when it loads. */
    @Test
    fun emptyUntilTheRowLoadsAndTheReaderIsTold() = bounded {
        open()
        val entity = User.entity()
        assertTrue(entity.value.value == null && entity.id.isEmpty() && !entity.isStale)
        login("1", "Ann")
        assertEquals("Ann", loaded(entity)?.name)
    }

    /** REL-24: logout empties the entity at once, and the next login loads the new user without the old one showing. */
    @Test
    fun logoutEmptiesItAndALaterLoginLoadsTheNewUser() = bounded {
        open()
        val entity = User.entity()
        login("1", "Ann")
        loaded(entity)
        DBlayer.logout()
        assertTrue(entity.value.value == null && entity.id.isEmpty())
        delay(300)
        assertNull(entity.value.value)
        assertTrue(User.entity() === entity)
        assertEquals(0uL, DBlayer.subscriptionCensus().observedQueries)
        assertNull(registry.currentSingleFeed)
        login("2", "Bob")
        DBlayer.resume()
        assertEquals("Bob", loaded(entity)?.name)
        assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
        val resumed = registry.currentSingleFeed
        DBlayer.resume()
        assertTrue(resumed === registry.currentSingleFeed)
    }

    /** A delivery of a feed that a newly opened database replaced writes nothing. */
    @Test
    fun aDeliveryOfAReplacedFeedNeverWrites() = bounded {
        open()
        val entity = User.entity()
        val old = registry.currentSingleFeed!!
        login("1", "Ann")
        loaded(entity)
        DBlayer.logout()
        open()
        assertTrue(User.entity() === entity && entity.value.value == null)
        registry.receive(old, Long.MAX_VALUE, User(id = "9", name = "Late", typing = false))
        assertNull(entity.value.value)
        login("2", "Bob")
        assertEquals("Bob", loaded(entity)?.name)
        assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
    }

    /** REL-27: 10 seconds without an event add no delivery and no subscription. */
    @Test
    fun idleTenSecondsCreatesNoWork() = bounded {
        open()
        val entity = User.entity()
        login("1", "Ann")
        loaded(entity)
        val feed = registry.currentSingleFeed!!
        val before = feed.delivered to DBlayer.subscriptionCensus()
        delay(10_000)
        assertEquals(before.first, feed.delivered)
        assertEquals(before.second.observedQueries, DBlayer.subscriptionCensus().observedQueries)
        assertNotNull(entity.value.value)
    }
}
