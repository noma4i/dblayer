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

private class HeldProfileTransport(override val base: MockTransport = MockTransport()) : ScriptedTransport, WrapsMock {
    override suspend fun mutate(request: TransportRequest): String = base.mutate(request)
    override suspend fun scriptedPut(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply = base.scriptedPut(url, headers, file, identity, progress)
    override suspend fun scriptedCable(url: String, events: CableEvents): CableOutcome = base.scriptedCable(url, events)
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
            closeForCleanup()
            transport.base.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}""")
            openSigned(Config(null, null), modelsJSON, transport, """[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
            val oldScope = AccountScope.current()
            val old = async { runCatching { User.profile.load(oldScope) } }
            transport.arrived.await()
            DBlayer.sessionReset()
            loginAgain()
            assertTrue(old.await().exceptionOrNull() is DbException.Interrupted)
            User.profile.load(AccountScope.current())
            transport.released.complete(Unit)
            transport.ended.await()
            assertEquals(listOf("B"), User.profile.read(AccountScope.current()).map { it.name })
            assertEquals(1, transport.base.calls.count { it.operationName == "UserProfile" })
        } finally {
            transport.released.complete(Unit)
            closeForCleanup()
        }
    }

    @Test
    fun resetClearsEphemeralValuesBeforeTheNextAccountReadsTheSameId() = bounded {
        try {
            closeForCleanup()
            val transport = MockTransport()
            for (name in listOf("A", "B")) {
                transport.enqueue("UserProfile", """{"data":{"me":{"__typename":"User","id":"1","name":"$name"}}}""")
            }
            openSigned(Config(null, null), modelsJSON, transport, """[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]""")
            User.profile.load(AccountScope.current())
            val old = User.profile.read(AccountScope.current()).single()
            old.set(typing = true, account = AccountScope.current())
            assertTrue(User.profile.read(AccountScope.current()).single().typing)

            DBlayer.sessionReset()
            loginAgain()
            User.profile.load(AccountScope.current())
            val current = User.profile.read(AccountScope.current()).single()

            assertEquals(old.id, current.id)
            assertEquals("B", current.name)
            assertFalse(current.typing)
        } finally {
            closeForCleanup()
        }
    }

    @Test
    fun theLoadThatRestoresAResetSessionReturnsItsOwnResult() = bounded {
        try {
            closeForCleanup()
            val transport = MockTransport()
            for ((user, region) in listOf("u1" to "r1", "u2" to "r2")) {
                transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"$user","name":"$user"}}}""")
                transport.enqueue("CurrentRegionQuery", """{"data":{"region":{"id":"$region","name":"$region"}}}""")
            }
            openSigned(Config(null, null), modelsJSON, transport, """[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"CurrentRegionQuery","document":"query CurrentRegionQuery { region { id name } }"}]""")
            CurrentUser.current.load(AccountScope.current())
            CurrentRegion.current.load(AccountScope.current())
            assertEquals(listOf("u1"), CurrentUser.current.read(AccountScope.current()).map { it.id })

            DBlayer.sessionReset()
            loginAgain()
            CurrentUser.current.load(AccountScope.current())
            CurrentRegion.current.load(AccountScope.current())

            assertEquals(4, transport.calls.size)
            assertEquals(listOf("u2"), CurrentUser.current.read(AccountScope.current()).map { it.id })
            assertEquals(listOf("r2"), CurrentRegion.current.read(AccountScope.current()).map { it.id })
        } finally {
            closeForCleanup()
        }
    }
}
