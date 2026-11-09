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
    private val alreadyAuthenticated = java.util.concurrent.atomic.AtomicBoolean(false)

    private suspend fun holdList(history: RowHistory): HeldList = HeldList(AccountScope.current().stream(Query(User.modelName, User)), history)

    private fun json(text: String): JsonObject = Json.parseToJsonElement(text).jsonObject

    private val server = SealedServer { path, body ->
        if (path == "/api/auth/login_link") {
            if (alreadyAuthenticated.getAndSet(false)) return@SealedServer json("""{"error":"ALREADY_AUTHENTICATED"}""")
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
        DBlayer.database.registerOperations(fixtureOperations)
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

    @Test
    fun theAppExitOrderEndsEveryHeldWaitWhenTheCredentialIsGone() = bounded {
        try {
            openSession()
            startSession("A")
            var entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
            val pending = async(start = CoroutineStart.UNDISPATCHED) { runCatching { entity.loaded() } }
            awaitWithin("the confirmed logout") { DBlayer.database.sessionCall("logout", "{}") }
            DBlayer.sessionReset()
            awaitWithin("the wait that was pending at the exit") { pending.await() }
            val late = runCatching { awaitWithin("the first-load wait after the exit") { entity.loaded() } }.exceptionOrNull()
            assertTrue("the wait after the exit must end Interrupted: $late", late is DbException.Interrupted)
        } finally {
            closeForCleanup()
        }
    }

    /** Every value delivered to a list held across a logout and the next login: none shows account A after the logout and the last one shows B. */
    @Test
    fun aLoginAfterALogoutNeverDeliversTheLastAccountToAHeldList() = bounded {
        try {
            openSession()
            startSession("A")
            val history = RowHistory()
            var live = holdList(history)
            try {
                eventually("the held list shows A") { history.states.lastOrNull()?.contains("A") == true }
                DBlayer.sessionReset()
                val seen = history.states.size
                startSession("B")
                live = holdList(history)
                eventually("the held list shows B") { history.states.lastOrNull()?.contains("B") == true }
                val after = history.states.drop(seen)
                assertTrue("account A came back after the logout: $after", after.none { "A" in it })
            } finally {
                live.cancel()
            }
        } finally {
            closeForCleanup()
        }
    }

    /** C08, R-9: the Android exit with a wait for the first load that the reset left pending at the server. Every call of the exit returns, the wait ends, the held entity is empty at once and the late answer of account A shows nowhere. */
    @Test
    fun theAppExitOrderEndsAPendingInitialWaitAndEveryCallOfTheExit() = bounded {
        try {
            openSession()
            startSession("A")
            var entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
            val history = RowHistory()
            var live = holdList(history)
            try {
                eventually("the held list shows A") { history.states.lastOrNull()?.contains("A") == true }
                server.hold("UserProfile")
                launch { runCatching { User.profile.refresh() } }
                eventually("the reread waits at the server") { server.parkedCount > 0 }
                val pending = async(start = CoroutineStart.UNDISPATCHED) { runCatching { entity.loaded() } }
                awaitWithin("the confirmed logout") { DBlayer.database.sessionCall("logout", "{}") }
                DBlayer.sessionReset()
                live.cancel()
                awaitWithin("the wait that was pending at the exit") { pending.await() }
                entity.until("the held entity ends with the session") { it.ended == EndCause.SessionEnded }
                server.hold(null)
                assertTrue("the late answer reached the held entity", entity.value?.name != "B")
                startSession("B")
                entity = heldUser()
                entity.until("the held entity shows B") { it.rows.firstOrNull()?.name == "B" }
            } finally {
                live.cancel()
                server.hold(null)
            }
        } finally {
            closeForCleanup()
        }
    }

    /** C12: A, B, A with a held entity and a held list. After each exit the late answer of the previous account arrives and neither shows it. */
    @Test
    fun anAccountSwitchABAKeepsAHeldListAndEntityFreeOfThePreviousAccount() = bounded {
        try {
            openSession()
            startSession("A")
            var entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
            val history = RowHistory()
            var live = holdList(history)
            try {
                eventually("the held list shows A") { history.states.lastOrNull()?.contains("A") == true }
                for ((next, previous) in listOf("B" to "A", "A" to "B")) {
                    server.hold("UserProfile")
                    launch { runCatching { User.profile.refresh() } }
                    eventually("the reread waits at the server") { server.parkedCount > 0 }
                    awaitWithin("the confirmed logout") { DBlayer.database.sessionCall("logout", "{}") }
                    DBlayer.sessionReset()
                    entity.until("the held entity ends with the session") { it.ended == EndCause.SessionEnded }
                    val seen = history.states.size
                    server.hold(null)
                    startSession(next)
                    entity = heldUser()
                    live = holdList(history)
                    entity.until("the held entity shows $next") { it.rows.firstOrNull()?.name == next }
                    eventually("the held list shows $next") { history.states.lastOrNull()?.contains(next) == true }
                    val after = history.states.drop(seen)
                    assertTrue("$previous came back after its exit: $after", after.none { previous in it })
                }
            } finally {
                live.cancel()
                server.hold(null)
            }
        } finally {
            closeForCleanup()
        }
    }

    /** Exit and login of the same account: the held entity empties and shows the account again, and the server sees a new read. */
    @Test
    fun aLogoutThenALoginOfTheSameAccountReadsItAgain() = bounded {
        try {
            openSession()
            startSession("A")
            var entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
            val reads = server.operationRequests("UserProfile")
            DBlayer.database.sessionCall("logout", "{}")
            DBlayer.sessionReset()
            entity.until("the held entity ends with the session") { it.ended == EndCause.SessionEnded }
            startSession("A")
            entity = heldUser()
            entity.until("the held entity shows A again") { it.rows.firstOrNull()?.name == "A" }
            assertTrue(server.operationRequests("UserProfile") > reads)
        } finally {
            closeForCleanup()
        }
    }

    /** C09, R-8: the local-only exit sends no unregister and no logout. The held entity empties and no read follows. */
    @Test
    fun aLocalOnlyExitSkipsTheRevokeAndStopsEveryRead() = bounded {
        try {
            openSession()
            startSession("A")
            var entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
            DBlayer.sessionReset()
            entity.until("the held entity ends with the session") { it.ended == EndCause.SessionEnded }
            val before = server.requests.size
            server.refuseEverything(true)
            assertEquals(before, server.requests.size)
            assertEquals(0, server.pathRequests("/api/logout"))
            server.refuseEverything(false)
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

    /** C19, R-5: the login is refused as already authenticated, the confirmed sign-out follows, the login repeats once and starts the session. */
    @Test
    fun anAlreadyAuthenticatedLoginSignsOutThenLogsInAgain() = bounded {
        try {
            openSession()
            alreadyAuthenticated.set(true)
            val refused = runCatching { DBlayer.database.sessionCall("login_link", """{"account":"A"}""") }.exceptionOrNull()
            assertTrue("expected ALREADY_AUTHENTICATED, got $refused", refused is DbException.Session && refused.code == SessionCode.ALREADY_AUTHENTICATED)
            val logins = server.pathRequests("/api/auth/login_link")
            val logout = runCatching { DBlayer.database.sessionCall("logout", "{}") }.exceptionOrNull()
            assertTrue("expected NO_CREDENTIAL, got $logout", logout is DbException.Session && logout.code == SessionCode.NO_CREDENTIAL)
            startSession("A")
            assertEquals(logins + 1, server.pathRequests("/api/auth/login_link"))
            var entity = heldUser()
            entity.until("the held entity shows A") { it.rows.firstOrNull()?.name == "A" }
        } finally {
            closeForCleanup()
        }
    }
}
