package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
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

    /** A refresh sent while the first request is in flight joins it, so the first answer must be applied before the test refreshes. */
    private suspend fun createEntity(): Entity<User> {
        transport.enqueue("UserProfile", reply = """{"data":{"me":null}}""")
        val entity = User.entity()
        eventually("the profile request of the new entity") { transport.calls.size == 1 }
        User.profile.load()
        return entity
    }

    private suspend fun refreshTo(id: String, name: String) {
        transport.enqueue("UserProfile", reply = """{"data":{"me":{"__typename":"User","id":"$id","name":"$name"}}}""")
        User.profile.refresh()
    }

    private suspend fun loaded(entity: Entity<User>) = withTimeout(5000) { entity.value.first { it != null } }

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

    /** The wait reuses the load the singleton started: one request, then it returns at once. */
    @Test
    fun theFirstLoadWaitOfASingletonSendsNoRequestOfItsOwn() = bounded {
        open()
        transport.enqueue("UserProfile", reply = """{"data":{"me":{"__typename":"User","id":"1","name":"Ann"}}}""")
        val entity = User.entity()
        withTimeout(5000) { entity.waitForInitialLoad() }
        assertEquals("Ann", entity.value.value?.name)
        eventually("the read of the singleton ends") { entity.read.value == EntityRead.Loaded }
        entity.waitForInitialLoad()
        assertEquals(1, transport.calls.size)
    }

    /** A failed first load reaches the waiter from the snapshot. */
    @Test
    fun aFailedFirstLoadOfASingletonReachesTheWaiter() = bounded {
        open()
        transport.enqueue("UserProfile", reply = """{"errors":[{"message":"denied"}]}""")
        val entity = User.entity()
        val failure = runCatching { withTimeout(5000) { entity.waitForInitialLoad() } }.exceptionOrNull()
        assertTrue(failure is EntityReadException)
        assertTrue(entity.read.value is EntityRead.Failed)
    }

    /** REL-01 for the singleton and REL-08: 10 readers share 1 object, 1 value and 1 core subscription. */
    @Test
    fun tenReadersCostOneSubscriptionAndSeeOneValue() = bounded {
        open()
        val readers = listOf(createEntity()) + List(9) { User.entity() }
        refreshTo("1", "Ann")
        loaded(readers[0])
        assertEquals(1, readers.map { System.identityHashCode(it) }.toSet().size)
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
        DBlayer.sessionReset()
        assertTrue(entity.value.value == null && entity.id.isEmpty())
        assertTrue(User.entity() === entity)
        assertEquals(0uL, DBlayer.subscriptionCensus().observedQueries)
    }

    /** REL-24: the next login after a logout loads the new user into the same entity, and a second resume keeps the feed. */
    @Test
    fun aLaterLoginLoadsTheNewUserIntoTheSameEntity() = bounded {
        open()
        val entity = createEntity()
        refreshTo("1", "Ann")
        loaded(entity)
        DBlayer.sessionReset()
        login("2", "Bob")
        DBlayer.resume()
        assertEquals("Bob", loaded(entity)?.name)
        assertEquals(1uL, DBlayer.subscriptionCensus().observedQueries)
        DBlayer.resume()
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
        val before = Triple(entity.snapshot.value.revision, DBlayer.subscriptionCensus().observedQueries, transport.calls.size)
        clock.advance(10_000L)
        DBlayer.database.flush()
        assertEquals(before.first, entity.snapshot.value.revision)
        assertEquals(before.second, DBlayer.subscriptionCensus().observedQueries)
        assertEquals(before.third, transport.calls.size)
        assertNotNull(entity.value.value)
    }
}
