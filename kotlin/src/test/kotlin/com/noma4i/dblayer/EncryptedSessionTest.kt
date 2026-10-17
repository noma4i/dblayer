package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

private const val SERVER_KEY = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtzb67l/cXul2uArFqeZXsMf4tImrLcaHb3uiVNp8mjjJrFK/zwUEEazm3pmriAbj1nBzSxQLuq3jNUeFHXoQxjwWmTdv+PoAypz4ZII6Dz6rknk290/ddmimc1Z4Mx0AkT4ffAA9WHiDtLPF6tKZWNsWF/SpJps9PWrIKNOKY78ZfCgfegrWfKwoNYExyZ0IuUUgG0lsRjEEE/U59GpbNBqdKtw7KaIGK47QRk6HWOkCQtyqqjsseOJLwdI0k6jDNmcH6DulPNmar9AbwsmpngD55P8y4JdH49Eq3Nr040gmg0Y1p1FZm5PbKeC4WgZZdmg1bxjKAa+MYP0fROi4dFz05/81fqFb6DWaXencb0p/Iz+IUfQiI/0EYNv1SfdTUHzGyHFAmQHeCUIKLSkNNg3uF/jqKthFecCnWEfTUWRWFsN/8/Nb65a1UI/L1hP/qd+T6xKfchqVBEf767yeHdgsy3GeuqTsgXUPkuA1DO9HPP1xWqa6orUNWkimSwxDAgMBAAE="
private const val SERVER_PIN = "fab4f816eb33917161942684230405ae47ad4df5b456bd973085599b1364342a"

private class RecordedHttp(private val status: UShort) : EncryptedHttp {
    val posts = CopyOnWriteArrayList<Pair<String, String>>()

    override suspend fun post(path: String, body: String): EncryptedReply {
        posts.add(path to body)
        return EncryptedReply(status, "")
    }

    val gets = CopyOnWriteArrayList<String>()

    override suspend fun get(path: String): EncryptedReply {
        gets.add(path)
        return EncryptedReply(503u, "")
    }
}

private class HeldCredential : CredentialStore {
    @Volatile var current: String? = null

    override fun get(): String? = current

    override fun set(credential: String) {
        current = credential
    }

    override fun clear() {
        current = null
    }
}

private suspend inline fun <reified Failure : Throwable> failsWith(block: () -> Unit) {
    try {
        block()
    } catch (error: Throwable) {
        if (error is Failure) return
        throw error
    }
    throw AssertionError("expected ${Failure::class.simpleName}")
}

class EncryptedSessionTest {
    @Test
    fun oversizedSessionCallsFailLocallyAndLeaveTheNextCallAvailable() = bounded {
        val http = RecordedHttp(400u)
        val database = Database.openEncrypted(
            Config(null, null, 32u), modelsJSON,
            EncryptedConfig("https://api.example.com", SERVER_KEY, SERVER_PIN),
            http, HeldCredential(), DatabaseTransport(cable = { null }),
        )
        try {
            database.sessionCall("otp", """{"phone":"${"x".repeat(260_000)}"}""")
            throw AssertionError("expected a local size refusal")
        } catch (error: DbException.RequestTooLarge) {
            org.junit.Assert.assertTrue(error.size > error.limit)
            assertEquals((256 * 1024).toULong(), error.limit)
        }
        assertEquals(0, http.posts.size)
        assertNull(database.sessionResume())
        try {
            database.sessionCall("otp", """{"phone":"+61400000000"}""")
            throw AssertionError("expected the scripted HTTP refusal")
        } catch (error: DbException.Http) {
            assertEquals(400.toUShort(), error.status)
        }
        assertEquals(1, http.posts.size)
    }

    @Test
    fun postsTheJoseTextToTheOriginPathAndReturnsAnyStatusUnchanged() = bounded {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(503).setBody("busy"))
            server.start()

            val reply = OkHttpEncryptedHttp(server.url("/").toString().trimEnd('/')).post("/api/v3/graphql", "a.b.c.d.e")

            assertEquals(503.toUShort() to "busy", reply.status to reply.body)
            val request = server.takeRequest()
            assertEquals("POST /api/v3/graphql", "${request.method} ${request.path}")
            assertEquals("application/jose", request.getHeader("Content-Type"))
            assertNull(request.getHeader("Authorization"))
            assertNull(request.getHeader("Cookie"))
            assertEquals("a.b.c.d.e", request.body.readUtf8())
        }
    }

    @Test
    fun readsThePublicConfigWithAGetThatCarriesNoBody() = bounded {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(200).setBody("{}"))
            server.start()

            val reply = OkHttpEncryptedHttp(server.url("/").toString().trimEnd('/')).get("/api/v3/config")

            assertEquals(200.toUShort() to "{}", reply.status to reply.body)
            val request = server.takeRequest()
            assertEquals("GET /api/v3/config", "${request.method} ${request.path}")
            assertNull(request.getHeader("Content-Type"))
        }
    }

    @Test
    fun aTransportFailureIsANetworkError() = bounded {
        val server = MockWebServer().apply { start() }
        val origin = server.url("/").toString().trimEnd('/')
        server.shutdown()

        failsWith<DbException.Network> { OkHttpEncryptedHttp(origin).post("/api/v3/logout", "x") }
    }

    @Test
    fun aTransportWithoutAnOperationRunRefusesQueriesAndMutations() = bounded {
        val transport = DatabaseTransport(cable = { null })
        val request = TransportRequest("query Q { q }", "Q", "{}")

        failsWith<DbException.Invalid> { transport.query(request) }
        failsWith<DbException.Invalid> { transport.mutate(request) }
    }

    @Test
    fun theCoreSendsSealedRequestsThroughTheAppHooksAndKeepsTheCredentialInTheAppStore() = bounded {
        val http = RecordedHttp(400u)
        val credentials = HeldCredential()
        val database = Database.openEncrypted(
            Config(null, null, 32u),
            modelsJSON,
            EncryptedConfig("https://api.example.com", SERVER_KEY, SERVER_PIN),
            http,
            credentials,
            DatabaseTransport(cable = { null }),
        )

        failsWith<DbException.Http> { database.sessionCall("otp", """{"phone":"+61400000000"}""") }
        failsWith<DbException.Network> { database.sessionCall("otp", """{"phone":"+61411111111"}""") }
        failsWith<DbException.Http> { database.serverConfig() }

        assertEquals(listOf("/api/v3/auth/otp"), http.posts.map { it.first })
        assertEquals(5, http.posts[0].second.split(".").size)
        assertEquals(43, credentials.current?.length)
        database.sessionReset()
        assertNull(credentials.current)
    }

    @Test
    fun openEncryptedMakesTheEncryptedDatabaseTheSharedOneAndTheConfigQueryReachesTheHook() = bounded {
        val http = RecordedHttp(400u)
        DBlayer.openEncrypted(
            EncryptedConfig("http://localhost:3001", SERVER_KEY, SERVER_PIN),
            HeldCredential(),
            DatabaseTransport(cable = { null }),
            modelsJSON,
            http = http,
        )

        failsWith<DbException.Http> { DBlayer.database.serverConfig("android", "1.2.3") }
        failsWith<DbException.Http> { DBlayer.database.serverConfig(null, null) }

        assertEquals(listOf("/api/v3/config?platform=android&version=1.2.3", "/api/v3/config"), http.gets)
    }
}
