package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
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

@OptIn(DBlayerTestingApi::class)
class LifecycleSessionTest {
    private val account = AtomicReference<String?>(null)

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
}
