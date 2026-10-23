package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
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
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
    }

    /** The server answers the profile request with this user. */
    private suspend fun login(id: String, name: String) {
        transport.enqueue("UserProfile", reply = """{"data":{"me":null}}""")
        User.profile.load()
        transport.enqueue("UserProfile", reply = """{"data":{"me":{"__typename":"User","id":"$id","name":"$name"}}}""")
        User.profile.refresh()
    }

    /** A new singleton entity requests the profile at once, so its reply is queued first and the test goes on after that request. */
    private suspend fun createEntity(): Entity<User> {
        transport.enqueue("UserProfile", reply = """{"data":{"me":null}}""")
        val entity = User.entity()
        eventually("the profile request of the new entity") { transport.calls.size == 1 }
        return entity
    }

    private suspend fun refreshTo(id: String, name: String) {
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
        val readers = listOf(createEntity()) + List(9) { User.entity() }
        val feed = registry.currentSingleFeed
        refreshTo("1", "Ann")
        loaded(readers[0])
        assertEquals(1, readers.map { System.identityHashCode(it) }.toSet().size)
        assertTrue(feed != null && feed === registry.currentSingleFeed)
        assertTrue(readers.all { it.value.value?.name == "Ann" && it.id == readers[0].value.value?.id })
        assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
    }

    @Test
    fun aNewSingletonEntityRequestsTheProfileByItself() = bounded {
        open()
        createEntity()

        assertEquals(listOf("UserProfile"), transport.calls.map { it.operationName })
    }

    /** The value is null before the first load and a collector is told when it loads. */
    @Test
    fun emptyUntilTheRowLoadsAndTheReaderIsTold() = bounded {
        open()
        val entity = createEntity()
        assertTrue(entity.value.value == null && entity.id.isEmpty() && !entity.isStale)
        refreshTo("1", "Ann")
        assertEquals("Ann", loaded(entity)?.name)
    }

    /** REL-24: logout empties the entity at once and stops its feed. */
    @Test
    fun logoutEmptiesTheEntityAtOnceAndStopsItsFeed() = bounded {
        open()
        val entity = createEntity()
        refreshTo("1", "Ann")
        loaded(entity)
        DBlayer.logout()
        assertTrue(entity.value.value == null && entity.id.isEmpty())
        assertTrue(User.entity() === entity)
        assertEquals(0uL, DBlayer.subscriptionCensus().observedQueries)
        assertNull(registry.currentSingleFeed)
    }

    /** REL-24: the next login after a logout loads the new user into the same entity, and a second resume keeps the feed. */
    @Test
    fun aLaterLoginLoadsTheNewUserIntoTheSameEntity() = bounded {
        open()
        val entity = createEntity()
        refreshTo("1", "Ann")
        loaded(entity)
        DBlayer.logout()
        login("2", "Bob")
        DBlayer.resume()
        assertEquals("Bob", loaded(entity)?.name)
        assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
        val resumed = registry.currentSingleFeed
        DBlayer.resume()
        assertTrue(resumed === registry.currentSingleFeed)
    }

    /** A delivery of a feed that a reset replaced writes nothing. */
    @Test
    fun aDeliveryOfAReplacedFeedNeverWrites() = bounded {
        open()
        val entity = createEntity()
        val old = registry.currentSingleFeed!!
        refreshTo("1", "Ann")
        loaded(entity)
        DBlayer.logout()
        transport.enqueue("UserProfile", reply = """{"data":{"me":null}}""")
        DBlayer.resume()
        eventually("the profile request after the resume") { transport.calls.size == 3 }
        assertTrue(User.entity() === entity && entity.value.value == null)
        registry.receive(old, Long.MAX_VALUE, User(id = "9", name = "Late", typing = false))
        assertNull(entity.value.value)
        refreshTo("2", "Bob")
        assertEquals("Bob", loaded(entity)?.name)
        assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
    }

    /** REL-27: 10 seconds of the core clock without an event add no delivery and no subscription. */
    @OptIn(DBlayerTestingApi::class)
    @Test
    fun idleTenSecondsCreatesNoWork() = bounded {
        DBlayer.close()
        val clock = DBlayer.openTesting(transport, modelsJSON)
        DBlayer.database.registerOperations("""[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
        val entity = createEntity()
        refreshTo("1", "Ann")
        loaded(entity)
        val feed = registry.currentSingleFeed!!
        val before = Triple(feed.delivered, DBlayer.subscriptionCensus().observedQueries, transport.calls.size)
        clock.advance(10_000L)
        DBlayer.database.flush()
        assertEquals(before.first, feed.delivered)
        assertEquals(before.second, DBlayer.subscriptionCensus().observedQueries)
        assertEquals(before.third, transport.calls.size)
        assertNotNull(entity.value.value)
    }
}
