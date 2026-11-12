package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference

private class LifecycleCredential : CredentialStore {
    @Volatile var current: String? = null

    override fun get(): String? = current

    override fun set(credential: String) {
        current = credential
    }

    override fun clear() {
        current = null
    }
}

private class RowHistory {
    val states = CopyOnWriteArrayList<List<String>>()
    private var projection = ListProjection<User>()

    fun begin() = synchronized(this) { projection = ListProjection<User>() }

    fun take(value: ReadValue) {
        synchronized(this) {
            projection.apply(value, User)?.let {
                projection = it
                states.add(it.rows.map { row -> row.name })
                TestPulse.pulse()
            }
        }
    }
}

private class HeldList(val stream: ValueStream, history: RowHistory) {
    private val reading: Job

    init {
        history.begin()
        reading = CoroutineScope(SupervisorJob() + Dispatchers.Default).launch { runCatching { stream.values.collect { history.take(it) } } }
    }

    suspend fun cancel() {
        reading.cancel()
        stream.cancel()
    }
}

@OptIn(DBlayerTestingApi::class)
class LifecycleSessionTest {
    private val account = AtomicReference<String?>(null)

    @Test
    fun bridge_prop_session_state_parity_c9() = bounded {
        runCatching { closeForCleanup() }
        val beforeOpen = runCatching { DBlayer.sessionStatus() }.exceptionOrNull()
        assertTrue(beforeOpen is DbException.NotOpen || beforeOpen is DbException.Closed)
        openSession()
        val initial = DBlayer.sessionStatus()
        assertEquals(SessionStanding.NONE, initial.phase)
        assertNull(initial.identity)
        startSession("A")
        val active = DBlayer.sessionStatus()
        assertEquals(SessionStanding.ACTIVE, active.phase)
        assertTrue(active.identity != null && active.epoch > initial.epoch)
        DBlayer.sessionReset()
        val reset = DBlayer.sessionStatus()
        assertEquals(SessionStanding.NONE, reset.phase)
        assertNull(reset.identity)
        assertTrue(reset.epoch > active.epoch)
        DBlayer.close()
        assertTrue(runCatching { DBlayer.sessionStatus() }.exceptionOrNull() is DbException.Closed)
    }

    private suspend fun holdList(history: RowHistory): HeldList = HeldList(AccountScope.current().stream(Query(User.modelName, User)), history)

    private fun json(text: String): JsonObject = Json.parseToJsonElement(text).jsonObject

    private val server = SealedServer { path, body ->
        if (path == "/api/auth/login_link") {
            val name = body.getValue("account").jsonPrimitive.content
            account.set(name)
            return@SealedServer json("""{"sessionId":"$name"}""")
        }
        if (path == "/api/logout") {
            account.set(null)
            return@SealedServer json("""{"revoked":true}""")
        }
        val name = account.get() ?: return@SealedServer json("""{"error":"UNAUTHENTICATED"}""")
        when (body["operationName"]?.jsonPrimitive?.content) {
            "SessionProfile" -> json("""{"data":{"sessionProfile":{"id":"$name"}}}""")
            "UserProfile" -> json("""{"data":{"me":{"__typename":"User","id":"1","name":"$name"}}}""")
            else -> json("""{"errors":[{"message":"unexpected operation"}]}""")
        }
    }

    /** The order of the app: sign-in call, session profile, profile refresh. */
    private suspend fun startSession(name: String) {
        DBlayer.database.sessionCall("login_link", """{"account":"$name"}""")
        DBlayer.call("SessionProfile", JsonObject(emptyMap()))
        User.profile.refresh()
    }

    private suspend fun openSession() {
        try {
            closeForCleanup()
        } catch (_: DbException.Closed) {
        }
        val encrypted = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", EncryptedOperationClass.AUTHENTICATED))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
        DBlayer.openTesting(MockTransport(), withRawCalls(modelsJSON), encrypted = encrypted, credentials = LifecycleCredential(), http = server)
        DBlayer.database.setOnline(true)
        val fixtureOperations = """[{"name":"UserProfile","document":"query UserProfile { me { id name } }"},{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}]"""
        server.registerDocuments(fixtureOperations)
        registerStandOperations(DBlayer.database, fixtureOperations)
    }

    /** REL-24 and the login hang: a login after a logout refreshes the held entity, no read reaches the server after the logout, and the next account never shows the last one. */
    @Test
    fun anAppLoginAfterALogoutRefreshesTheHeldEntityAndNoReadFollowsTheLogout() = bounded {
        try {
            openSession()
            startSession("A")
            var entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
            val before = server.requests.size
            DBlayer.sessionReset()
            entity.until("the held entity ends with the session") { it.ended == EndCause.SessionEnded }
            server.refuseEverything(true)
            assertEquals(before, server.requests.size)
            server.refuseEverything(false)
            startSession("B")
            entity = heldUser()
            entity.until("the held entity shows B") { it.rows.firstOrNull()?.name == "B" }
        } finally {
            closeForCleanup()
        }
    }

    /** The real exit of the app: the confirmed logout call, the session reset, the data clear, then the next login. No read follows the exit and account A never shows after it. */
    @Test
    fun aConfirmedLogoutThenASessionResetAndAClearNeverShowsTheAccountToTheNextLogin() = bounded {
        try {
            openSession()
            startSession("A")
            var entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
            DBlayer.database.sessionCall("logout", "{}")
            DBlayer.sessionReset()
            entity.until("the held entity ends with the session") { it.ended == EndCause.SessionEnded }
            val before = server.requests.size
            server.refuseEverything(true)
            assertEquals(before, server.requests.size)
            server.refuseEverything(false)
            startSession("B")
            entity = heldUser()
            entity.until("the held entity shows B") { it.rows.firstOrNull()?.name == "B" }
        } finally {
            closeForCleanup()
        }
    }

    /** C11: a call that the server never answers ends when the database closes. */
    @Test
    fun closeEndsACallThatTheServerNeverAnswers() = bounded {
        try {
            openSession()
            startSession("A")
            server.hold("UserProfile")
            val ended = async(start = CoroutineStart.UNDISPATCHED) { runCatching { User.profile.refresh() } }
            eventually("the request waits at the server") { server.parkedCount > 0 }
            closeForCleanup()
            awaitWithin("the call that the server never answered") { ended.await() }
            Unit
        } finally {
            server.hold(null)
            runCatching { closeForCleanup() }
        }
    }
}
