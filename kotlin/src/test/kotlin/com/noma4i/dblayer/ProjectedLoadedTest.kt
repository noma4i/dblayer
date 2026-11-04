package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.Presence
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

@OptIn(DBlayerTestingApi::class)
class ProjectedLoadedTest {
    @Test
    fun cancellingOnlyTheLoadWaitLeavesTheReaderRunning() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            val gate = transport.hold("ContainerNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"ContainerNode","document":"query ContainerNode(${'$'}id: ID!) { node(id: ${'$'}id) { ... on Container { id title shareUrl isPublic } } }"}]""")
            val held = Projected(AccountScope.current().entity(Container, "c1", EntityFetch.FORCE), Container, scope)
            try {
                gate.entered()
                val waiting = async(start = CoroutineStart.UNDISPATCHED) { held.loaded() }
                assertTrue(waiting.isActive)
                waiting.cancelAndJoin()
                gate.release()
                held.loaded()
                assertEquals("one", held.value?.title)
            } finally {
                gate.release()
                held.close()
            }
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    @Test
    fun aReceiptRevisionWaitsUntilTheUpdatedRowIsShown() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            reopen(Config(null, null, 32u), modelsJSON, MockTransport())
            val created = Presence.create(userId = "u1", room = "old")
            val row = requireNotNull(created.optimisticValue)
            val held = Projected(AccountScope.current().stream(Presence.where(Condition.equal("userId", jsonValue("u1")))), Presence, scope)
            held.loaded()
            val receipt = row.update(room = "new")

            held.shown(receipt.revision)
            assertEquals("new", held.projection.value.rows.first { it.id == row.id }.room)
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    @Test
    fun aColdProfileSettlesWhenEmptyOrFailed() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            for (failed in listOf(false, true)) {
                DBlayer.close()
                val transport = MockTransport()
                val down = if (failed) transport.hold("CurrentUserQuery", failure = DbException.Network("down")) else null
                if (!failed) transport.enqueue("CurrentUserQuery", """{"data":{"user":null}}""")
                DBlayer.openTesting(transport, modelsJSON)
                DBlayer.database.registerOperations("""[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]""")
                val held = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, scope)
                if (down != null) networkDrops(during = down)
                val error = runCatching { held.loaded() }.exceptionOrNull()
                if (failed) assertTrue(error is DbException.Network) else assertNull(error)
                assertNull(held.value)
                assertEquals(if (failed) LivePhase.FAILED else LivePhase.EMPTY, held.projection.value.phase)
                if (failed) assertTrue(held.projection.value.error is DbException.Network)
                held.close()
            }
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    @Test
    fun cancellingAReaderBeforeItsFirstLoadInterruptsTheWait() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            val gate = transport.hold("ContainerNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"ContainerNode","document":"query ContainerNode(${'$'}id: ID!) { node(id: ${'$'}id) { ... on Container { id title shareUrl isPublic } } }"}]""")
            val held = Projected(AccountScope.current().entity(Container, "c1", EntityFetch.FORCE), Container, scope)
            gate.entered()
            held.close()
            gate.release()
            assertTrue(runCatching { held.loaded() }.exceptionOrNull() is DbException.Interrupted)
            assertNull(held.value)
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    @Test
    fun aHeldEntityEndsItsLoadWaitAfterSessionReset() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"ContainerNode","document":"query ContainerNode(${'$'}id: ID!) { node(id: ${'$'}id) { ... on Container { id title shareUrl isPublic } } }"}]""")
            val held = Projected(AccountScope.current().entity(Container, "c1", EntityFetch.FORCE), Container, scope)
            held.loaded()
            DBlayer.sessionReset()
            assertEquals(StreamEnd.SESSION_ENDED, held.projection.first { it.ended != null }.ended)
            assertTrue(runCatching { held.loaded() }.exceptionOrNull() is DbException.Interrupted)
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    @Test
    fun anUnknownAssociationReportsItsErrorAfterTheStreamEnds() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            reopen(Config(null, null, 32u), modelsJSON, MockTransport())
            val association = Association<Presence, User>("missing", User)
            val held = Projected(AccountScope.current().related(Presence.where(Condition.equal("room", jsonValue("r"))), association), User, scope)
            val ended = held.projection.first { it.ended != null }
            assertEquals(StreamEnd.INVALID, ended.ended)
            val error = runCatching { held.loaded() }.exceptionOrNull()
            assertTrue("unexpected error: $error", error is DbException.Invalid && error.message.orEmpty().contains("missing"))
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    @Test
    fun aCachedEntityRemainsLoadedWhenItsInitialNetworkReadFails() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
            val down = transport.hold("ContainerNode", failure = DbException.Network("down"))
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"ContainerNode","document":"query ContainerNode(${'$'}id: ID!) { node(id: ${'$'}id) { ... on Container { id title shareUrl isPublic } } }"}]""")
            val first = Projected(AccountScope.current().entity(Container, "c1", EntityFetch.FORCE), Container, scope)
            first.loaded()
            first.close()
            val held = Projected(AccountScope.current().entity(Container, "c1", EntityFetch.FORCE), Container, scope)
            networkDrops(during = down)
            held.loaded()
            assertEquals("one", held.value?.title)
            assertEquals(LivePhase.LOADED, held.projection.value.phase)
            assertTrue(held.projection.value.error is DbException.Network)
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    @Test
    fun theValueOfAHeldStreamIsThereWhenLoadedReturnsAndARefreshStillAsksTheServer() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            repeat(2) { transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"u1"}}}""") }
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]""")
            val held = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, scope)

            held.loaded()
            assertEquals("u1", held.value?.id)
            held.loaded()
            assertEquals("u1", held.value?.id)
            assertEquals(1, transport.calls.count { it.operationName == "CurrentUserQuery" })
            CurrentUser.current.refresh()
            assertEquals(2, transport.calls.count { it.operationName == "CurrentUserQuery" })
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }

    /** Requirement: `loaded()` throws `Interrupted` when the reader ends before the projection showed the rows of the loaded revision. The reader cannot run until the stream is closed. */
    @Test
    fun loadedThrowsWhenTheReaderEndsBeforeItShowedTheRows() = bounded {
        val gate = CountDownLatch(1)
        val executor = Executors.newSingleThreadExecutor()
        val blocked = CoroutineScope(SupervisorJob() + executor.asCoroutineDispatcher())
        try {
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"u1"}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]""")
            executor.execute { gate.await() }
            val held = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, blocked)
            val waiting = async(Dispatchers.Default) { runCatching { held.loaded() }.exceptionOrNull() }
            held.close()
            gate.countDown()
            assertTrue("loaded() ended with ${waiting.await()}", waiting.await() is DbException.Interrupted)
            assertNull(held.value)
        } finally {
            gate.countDown()
            blocked.cancel()
            executor.shutdown()
            DBlayer.close()
        }
    }

    /** Requirement: for a stream of one entity that the library loads from the server, the row is visible when `loaded()` returns. */
    @Test
    fun theRowOfAnEntityStreamThatLoadsFromTheServerIsThereWhenLoadedReturns() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            DBlayer.close()
            val transport = MockTransport()
            transport.enqueue("ContainerNode", """{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"ContainerNode","document":"query ContainerNode(${'$'}id: ID!) { node(id: ${'$'}id) { ... on Container { id title shareUrl isPublic } } }"}]""")
            val held = Projected(AccountScope.current().entity(Container, "c1", EntityFetch.FORCE), Container, scope)
            held.loaded()
            assertEquals("one", held.value?.title)
            held.close()
        } finally {
            scope.cancel()
            DBlayer.close()
        }
    }
}
