package com.noma4i.dblayer

import com.noma4i.dblayer.models.CurrentRegion
import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

private class HeldProfileTransport(val base: MockTransport = MockTransport()) : Transport by base {
    val arrived = CompletableDeferred<Unit>()
    val released = CompletableDeferred<Unit>()
    val ended = CompletableDeferred<Unit>()
    private val first = AtomicBoolean(true)

    override suspend fun query(request: TransportRequest): String {
        if (!first.getAndSet(false)) return base.query(request)
        try {
            arrived.complete(Unit)
            released.await()
            return """{"data":{"me":{"__typename":"User","id":"1","name":"A"}}}"""
        } finally {
            ended.complete(Unit)
        }
    }
}

@OptIn(DBlayerTestingApi::class)
class SessionRestoreTest {
    @Test
    fun aHeldLoadEndsAtResetAndCannotWriteIntoTheNextAccount() = bounded {
        val transport = HeldProfileTransport()
        try {
            DBlayer.close()
            transport.base.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}""")
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
            val oldEpoch = DBlayer.database.sessionEpoch()
            val oldScope = AccountScope.current()
            val old = async { runCatching { User.profile.load(oldScope) } }
            transport.arrived.await()
            DBlayer.sessionReset()
            assertTrue(runCatching { DBlayer.database.resume(oldEpoch) }.exceptionOrNull() is DbException.Interrupted)
            assertTrue(old.await().exceptionOrNull() is DbException.Interrupted)
            User.profile.load(AccountScope.current())
            transport.released.complete(Unit)
            transport.ended.await()
            assertEquals(listOf("B"), User.profile.read(AccountScope.current()).map { it.name })
            assertEquals(1, transport.base.calls.count { it.operationName == "UserProfile" })
        } finally {
            transport.released.complete(Unit)
            DBlayer.close()
        }
    }

    @Test
    fun resetClearsEphemeralValuesBeforeTheNextAccountReadsTheSameId() = bounded {
        try {
            DBlayer.close()
            val transport = MockTransport()
            for (name in listOf("A", "B")) {
                transport.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"1","name":"$name"}}}""")
            }
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
            User.profile.load(AccountScope.current())
            val old = User.profile.read(AccountScope.current()).single()
            old.set(typing = true, account = AccountScope.current())
            assertTrue(User.profile.read(AccountScope.current()).single().typing)

            DBlayer.sessionReset()
            User.profile.load(AccountScope.current())
            val current = User.profile.read(AccountScope.current()).single()

            assertEquals(old.id, current.id)
            assertEquals("B", current.name)
            assertFalse(current.typing)
        } finally {
            DBlayer.close()
        }
    }

    @Test
    fun theLoadThatRestoresAResetSessionReturnsItsOwnResult() = bounded {
        try {
            DBlayer.close()
            val transport = MockTransport()
            for ((user, region) in listOf("u1" to "r1", "u2" to "r2")) {
                transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"$user","name":"$user"}}}""")
                transport.enqueue("CurrentRegionQuery", """{"data":{"region":{"id":"$region","name":"$region"}}}""")
            }
            DBlayer.openTesting(transport, modelsJSON)
            DBlayer.database.registerOperations("""[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"CurrentRegionQuery","document":"query CurrentRegionQuery { region { id name } }"}]""")
            CurrentUser.current.load(AccountScope.current())
            CurrentRegion.current.load(AccountScope.current())
            assertEquals(listOf("u1"), CurrentUser.current.read(AccountScope.current()).map { it.id })

            DBlayer.sessionReset()
            CurrentUser.current.load(AccountScope.current())
            CurrentRegion.current.load(AccountScope.current())

            assertEquals(4, transport.calls.size)
            assertEquals(listOf("u2"), CurrentUser.current.read(AccountScope.current()).map { it.id })
            assertEquals(listOf("r2"), CurrentRegion.current.read(AccountScope.current()).map { it.id })
        } finally {
            DBlayer.close()
        }
    }
}
