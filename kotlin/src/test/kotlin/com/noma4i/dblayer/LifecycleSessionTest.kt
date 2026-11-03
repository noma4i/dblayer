package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.coroutines.CoroutineStart
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

private class RowHistory : QueryListener {
    val states = CopyOnWriteArrayList<List<String>>()
    private val rows = ArrayList<WireRow>()

    override fun changed(change: LiveChange) {
        synchronized(rows) {
            if (change.full) rows.clear() else rows.removeAll { it.id in change.removed }
            for (row in change.rows) {
                rows.removeAll { it.id == row.id }
                rows.add(row)
            }
            states.add(rows.flatMap { row -> row.cells.mapNotNull { (it as? Cell.Text)?.value } })
        }
    }
}

@OptIn(DBlayerTestingApi::class)
class LifecycleSessionTest {
    private val account = AtomicReference<String?>(null)
    private val alreadyAuthenticated = java.util.concurrent.atomic.AtomicBoolean(false)

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

    /** The order of the app: sign-in call, data logout, session profile, profile refresh. */
    private suspend fun startSession(name: String) {
        DBlayer.database.sessionCall("login_link", """{"account":"$name"}""")
        DBlayer.logout()
        DBlayer.run("SessionProfile", JsonObject(emptyMap()))
        User.profile.refresh()
    }

    private suspend fun openSession() {
        DBlayer.close()
        val encrypted = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", false))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
        DBlayer.openTesting(MockTransport(), modelsJSON, encrypted = encrypted, credentials = LifecycleCredential(), http = server)
        DBlayer.database.registerOperations("""[{"name":"UserProfile","document":"query UserProfile { me { id name } }"},{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}]""")
    }

    /** REL-24 and the login hang: a login after a logout refreshes the held entity, no read reaches the server after the logout, and the next account never shows the last one. */
    @Test
    fun anAppLoginAfterALogoutRefreshesTheHeldEntityAndNoReadFollowsTheLogout() = bounded {
        try {
            openSession()
            startSession("A")
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
            val before = server.requests.size
            DBlayer.logout()
            assertNull(entity.value.value)
            server.refuseEverything(true)
            DBlayer.resetForTesting()
            DBlayer.settleForTesting()
            assertEquals(before, server.requests.size)
            server.refuseEverything(false)
            startSession("B")
            eventually("the held entity shows B") { entity.value.value?.name == "B" }
        } finally {
            DBlayer.close()
        }
    }

    /** The real exit of the app: the confirmed logout call, the session reset, the data clear, then the next login. No read follows the exit and account A never shows after it. */
    @Test
    fun aConfirmedLogoutThenASessionResetAndAClearNeverShowsTheAccountToTheNextLogin() = bounded {
        try {
            openSession()
            startSession("A")
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
            DBlayer.database.sessionCall("logout", "{}")
            DBlayer.sessionReset()
            DBlayer.logout()
            assertNull(entity.value.value)
            val before = server.requests.size
            server.refuseEverything(true)
            DBlayer.settleForTesting()
            assertEquals(before, server.requests.size)
            server.refuseEverything(false)
            startSession("B")
            eventually("the held entity shows B") { entity.value.value?.name == "B" }
        } finally {
            DBlayer.close()
        }
    }

    @Test
    fun theAppExitOrderEndsEveryHeldWaitWhenTheCredentialIsGone() = bounded {
        try {
            openSession()
            startSession("A")
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
            val holding = launch(start = CoroutineStart.UNDISPATCHED) {
                val changes = DBlayer.database.changes("""{"model":"User"}""")
                while (changes.next() != null) { }
            }
            val pending = async(start = CoroutineStart.UNDISPATCHED) { runCatching { entity.waitForInitialLoad() } }
            awaitWithin("the confirmed logout") { DBlayer.database.sessionCall("logout", "{}") }
            DBlayer.sessionReset()
            awaitWithin("the cancel of the coroutine that holds the stream") { holding.cancelAndJoin() }
            awaitWithin("the data logout") { DBlayer.logout() }
            awaitWithin("the wait that was pending at the exit") { pending.await() }
            val late = runCatching { awaitWithin("the first-load wait after the exit") { entity.waitForInitialLoad() } }.exceptionOrNull()
            assertTrue("the wait after the exit must end Interrupted: $late", late is DbException.Interrupted)
        } finally {
            DBlayer.close()
        }
    }

    /** The session stays, only the held reads start over: the reset rereads at once, no login follows, and the held entity shows the data the server holds now. */
    @Test
    fun resetForTestingWithTheSessionKeptRereadsAtOnceAndTheHeldEntityGetsFreshValues() = bounded {
        try {
            openSession()
            startSession("A")
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
            val logins = server.pathRequests("/api/auth/login_link")
            val reads = server.operationRequests("UserProfile")
            account.set("A2")
            DBlayer.resetForTesting()
            DBlayer.settleForTesting()
            assertTrue(server.operationRequests("UserProfile") > reads)
            eventually("the held entity shows A2") { entity.value.value?.name == "A2" }
            assertEquals(logins, server.pathRequests("/api/auth/login_link"))
        } finally {
            DBlayer.close()
        }
    }

    /** Every value delivered to a list held across a logout and the next login: none shows account A after the logout and the last one shows B. */
    @Test
    fun aLoginAfterALogoutNeverDeliversTheLastAccountToAHeldList() = bounded {
        try {
            openSession()
            startSession("A")
            val states = CopyOnWriteArrayList<List<String>>()
            val rows = ArrayList<WireRow>()
            val live = DBlayer.database.liveQuery("""{"model":"User"}""", object : QueryListener {
                override fun changed(change: LiveChange) {
                    synchronized(rows) {
                        if (change.full) {
                            rows.clear()
                        } else {
                            rows.removeAll { it.id in change.removed }
                        }
                        for (row in change.rows) {
                            rows.removeAll { it.id == row.id }
                            rows.add(row)
                        }
                        states.add(rows.flatMap { row -> row.cells.mapNotNull { (it as? Cell.Text)?.value } })
                    }
                }
            })
            try {
                eventually("the held list shows A") { states.lastOrNull()?.contains("A") == true }
                val seen = states.size
                DBlayer.logout()
                startSession("B")
                eventually("the held list shows B") { states.lastOrNull()?.contains("B") == true }
                DBlayer.settleForTesting()
                val after = states.drop(seen)
                assertTrue("account A came back after the logout: $after", after.none { "A" in it })
            } finally {
                live.cancel()
            }
        } finally {
            DBlayer.close()
        }
    }

    /** C08, R-9: the Android exit with a wait for the first load that the reset left pending at the server. Every call of the exit returns, the wait ends, the held entity is empty at once and the late answer of account A shows nowhere. */
    @Test
    fun theAppExitOrderEndsAPendingInitialWaitAndEveryCallOfTheExit() = bounded {
        try {
            openSession()
            startSession("A")
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
            val history = RowHistory()
            val live = DBlayer.database.liveQuery("""{"model":"User"}""", history)
            try {
                eventually("the held list shows A") { history.states.lastOrNull()?.contains("A") == true }
                server.hold("UserProfile")
                DBlayer.resetForTesting()
                eventually("the reread waits at the server") { server.parkedCount > 0 }
                val pending = async(start = CoroutineStart.UNDISPATCHED) { runCatching { entity.waitForInitialLoad() } }
                awaitWithin("the confirmed logout") { DBlayer.database.sessionCall("logout", "{}") }
                DBlayer.sessionReset()
                live.cancel()
                awaitWithin("the data logout") { DBlayer.logout() }
                awaitWithin("the wait that was pending at the exit") { pending.await() }
                assertNull(entity.value.value)
                server.hold(null)
                DBlayer.settleForTesting()
                assertNull("the late answer of account A reached the held entity", entity.value.value)
                startSession("B")
                eventually("the held entity shows B") { entity.value.value?.name == "B" }
            } finally {
                live.cancel()
                server.hold(null)
            }
        } finally {
            DBlayer.close()
        }
    }

    /** C12: A, B, A with a held entity and a held list. After each exit the late answer of the previous account arrives and neither shows it. */
    @Test
    fun anAccountSwitchABAKeepsAHeldListAndEntityFreeOfThePreviousAccount() = bounded {
        try {
            openSession()
            startSession("A")
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
            val history = RowHistory()
            val live = DBlayer.database.liveQuery("""{"model":"User"}""", history)
            try {
                eventually("the held list shows A") { history.states.lastOrNull()?.contains("A") == true }
                for ((next, previous) in listOf("B" to "A", "A" to "B")) {
                    server.hold("UserProfile")
                    DBlayer.resetForTesting()
                    eventually("the reread waits at the server") { server.parkedCount > 0 }
                    awaitWithin("the confirmed logout") { DBlayer.database.sessionCall("logout", "{}") }
                    DBlayer.sessionReset()
                    awaitWithin("the data logout") { DBlayer.logout() }
                    assertNull(entity.value.value)
                    val seen = history.states.size
                    server.hold(null)
                    startSession(next)
                    eventually("the held entity shows $next") { entity.value.value?.name == next }
                    DBlayer.settleForTesting()
                    eventually("the held list shows $next") { history.states.lastOrNull()?.contains(next) == true }
                    val after = history.states.drop(seen)
                    assertTrue("$previous came back after its exit: $after", after.none { previous in it })
                }
            } finally {
                live.cancel()
                server.hold(null)
            }
        } finally {
            DBlayer.close()
        }
    }

    /** Exit and login of the same account: the held entity empties and shows the account again, and the server sees a new read. */
    @Test
    fun aLogoutThenALoginOfTheSameAccountReadsItAgain() = bounded {
        try {
            openSession()
            startSession("A")
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
            val reads = server.operationRequests("UserProfile")
            DBlayer.database.sessionCall("logout", "{}")
            DBlayer.sessionReset()
            DBlayer.logout()
            assertNull(entity.value.value)
            startSession("A")
            eventually("the held entity shows A again") { entity.value.value?.name == "A" }
            assertTrue(server.operationRequests("UserProfile") > reads)
        } finally {
            DBlayer.close()
        }
    }

    /** C09, R-8: the local-only exit sends no unregister and no logout. The held entity empties and no read follows. */
    @Test
    fun aLocalOnlyExitSkipsTheRevokeAndStopsEveryRead() = bounded {
        try {
            openSession()
            startSession("A")
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
            DBlayer.sessionReset()
            DBlayer.logout()
            assertNull(entity.value.value)
            val before = server.requests.size
            server.refuseEverything(true)
            DBlayer.settleForTesting()
            assertEquals(before, server.requests.size)
            assertEquals(0, server.pathRequests("/api/logout"))
            server.refuseEverything(false)
        } finally {
            DBlayer.close()
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
            DBlayer.close()
            awaitWithin("the call that the server never answered") { ended.await() }
            Unit
        } finally {
            server.hold(null)
            DBlayer.close()
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
            DBlayer.database.sessionCall("logout", "{}")
            startSession("A")
            assertEquals(logins + 1, server.pathRequests("/api/auth/login_link"))
            val entity = User.entity()
            eventually("the held entity shows A") { entity.value.value?.name == "A" }
        } finally {
            DBlayer.close()
        }
    }
}
