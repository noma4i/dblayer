package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Test
import java.io.File
import java.nio.file.Files
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.cancelAndJoin

private const val KEY = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtzb67l/cXul2uArFqeZXsMf4tImrLcaHb3uiVNp8mjjJrFK/zwUEEazm3pmriAbj1nBzSxQLuq3jNUeFHXoQxjwWmTdv+PoAypz4ZII6Dz6rknk290/ddmimc1Z4Mx0AkT4ffAA9WHiDtLPF6tKZWNsWF/SpJps9PWrIKNOKY78ZfCgfegrWfKwoNYExyZ0IuUUgG0lsRjEEE/U59GpbNBqdKtw7KaIGK47QRk6HWOkCQtyqqjsseOJLwdI0k6jDNmcH6DulPNmar9AbwsmpngD55P8y4JdH49Eq3Nr040gmg0Y1p1FZm5PbKeC4WgZZdmg1bxjKAa+MYP0fROi4dFz05/81fqFb6DWaXencb0p/Iz+IUfQiI/0EYNv1SfdTUHzGyHFAmQHeCUIKLSkNNg3uF/jqKthFecCnWEfTUWRWFsN/8/Nb65a1UI/L1hP/qd+T6xKfchqVBEf767yeHdgsy3GeuqTsgXUPkuA1DO9HPP1xWqa6orUNWkimSwxDAgMBAAE="
private const val PIN = "fab4f816eb33917161942684230405ae47ad4df5b456bd973085599b1364342a"
private val OTHER_PIN = "0".repeat(64)
private const val OTP = """{"phone":"+61400000000"}"""

private class ScriptedHttp : EncryptedHttp {
    val paths = CopyOnWriteArrayList<String>()
    @Volatile var status: UShort = 401u
    @Volatile var offline = false

    override suspend fun post(path: String, body: String): EncryptedReply {
        paths.add(path)
        if (offline) throw DbException.Network("offline")
        return EncryptedReply(status, "")
    }

}

private class CountedCredential(initial: String? = null) : CredentialStore {
    @Volatile var current: String? = initial
    @Volatile var sets = 0
    @Volatile var clears = 0

    override fun get(): String? = current

    override fun set(credential: String) {
        current = credential
        sets += 1
    }

    override fun clear() {
        current = null
        clears += 1
    }
}

private class ClosingSocket : CableSocket {
    override suspend fun open() {}
    override fun close() {}
    override suspend fun send(text: String) {}
    override suspend fun receive(): String = throw CableSocketException(CableSocketException.Reason.Closed)
}

private suspend fun cancelOnRetry(database: Database, operation: String, body: String) = coroutineScope {
    val nextAttempt = CompletableDeferred<ULong>()
    database.setRetryListener(object : RetryListener {
        override fun retrying(attempt: ULong, delayMs: ULong, reason: String) { nextAttempt.complete(attempt) }
    })
    val call = launch { database.sessionCall(operation, body) }
    assertEquals(1uL, nextAttempt.await())
    call.cancelAndJoin()
    database.setRetryListener(null)
}

private suspend inline fun <reified Failure : Throwable> rejects(block: () -> Unit) {
    try {
        block()
    } catch (error: Throwable) {
        if (error is Failure) return
        throw error
    }
    throw AssertionError("expected ${Failure::class.simpleName}")
}

private fun open(http: ScriptedHttp, credentials: CountedCredential, path: String? = null, pin: String = PIN) =
    Database.open(config = Config(path, null, 32u), modelsJson = modelsJSON, transport = DatabaseTransport(cable = { null }), encrypted = EncryptedConfig("https://api.example.com", KEY, pin, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("otp" to EncryptedSessionOperation("/api/auth/otp", false), "confirm" to EncryptedSessionOperation("/api/auth/confirm", false), "social" to EncryptedSessionOperation("/api/auth/social", false), "ticket" to EncryptedSessionOperation("/api/ticket", true))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel"), credentials = credentials, http = http)

private inline fun withDirectory(block: (File) -> Unit) {
    val directory = Files.createTempDirectory("dblayer-session").toFile()
    try {
        block(directory)
    } finally {
        directory.deleteRecursively()
    }
}

class SessionGuardsTest {
    @Test
    fun aPinMismatchAtOpenNeverTouchesTheCredentialStore() = bounded {
        val http = ScriptedHttp()
        val credentials = CountedCredential("kept")
        rejects<DbException.Invalid> { open(http, credentials, pin = OTHER_PIN) }
        open(http, credentials)

        assertEquals("kept", credentials.current)
        assertEquals(0 to 0, credentials.sets to credentials.clears)
    }

    @Test
    fun httpRefusalsKeepTheCredentialAndTheRequestStaysPendingForResume() = bounded {
        for (status in listOf<UShort>(401u, 403u, 423u, 451u)) {
            val http = ScriptedHttp()
            val credentials = CountedCredential()
            val database = open(http, credentials)
            http.status = status
            rejects<DbException.Http> { database.sessionCall("otp", OTP) }
            val issued = credentials.current

            rejects<DbException.Http> { database.sessionResume() }

            assertEquals("$status", listOf("/api/auth/otp", "/api/auth/otp"), http.paths.toList())
            assertEquals("$status", issued to (1 to 0), credentials.current to (credentials.sets to credentials.clears))
        }
    }

    @Test
    fun resumeAfterARestartSendsThePendingRequestAndNothingWithoutOne() = bounded {
        withDirectory { directory ->
            val path = File(directory, "session.sqlite").path
            val http = ScriptedHttp()
            val credentials = CountedCredential()
            val first = open(http, credentials, path)
            http.offline = true
            cancelOnRetry(first, "otp", OTP)
            http.offline = false

            val restarted = open(http, credentials, path)
            rejects<DbException.Http> { restarted.sessionResume() }
            assertEquals(listOf("/api/auth/otp", "/api/auth/otp"), http.paths.toList())

            val quiet = ScriptedHttp()
            val nothing = open(quiet, CountedCredential(), File(directory, "fresh.sqlite").path)
            assertNull(nothing.sessionResume())
            assertEquals(emptyList<String>(), quiet.paths.toList())
        }
    }

    @Test
    fun aLogoutWhoseAnswerWasLostIsResentAfterARestartAndKeepsTheCredentialUntilItIsRevoked() = bounded {
        withDirectory { directory ->
            val path = File(directory, "session.sqlite").path
            val http = ScriptedHttp()
            val credentials = CountedCredential()
            val first = open(http, credentials, path)
            http.offline = true
            cancelOnRetry(first, "logout", "{}")
            http.offline = false

            val restarted = open(http, credentials, path)
            rejects<DbException.Http> { restarted.sessionResume() }

            assertEquals(listOf("/api/logout", "/api/logout"), http.paths.toList())
            assertEquals(43 to (1 to 0), credentials.current?.length to (credentials.sets to credentials.clears))
        }
    }

    @Test
    fun aCredentialSavedBeforeItsSessionRowIsReplacedByTheNextCall() = bounded {
        val http = ScriptedHttp()
        val credentials = CountedCredential("saved-before-the-crash")
        val database = open(http, credentials)

        rejects<DbException.Http> { database.sessionCall("otp", OTP) }

        assertNotEquals("saved-before-the-crash", credentials.current)
        assertEquals(43 to (1 to 0), credentials.current?.length to (credentials.sets to credentials.clears))
    }

    @Test
    fun aClosedCableLeavesTheCredentialInTheAppStore() = bounded {
        val http = ScriptedHttp()
        val credentials = CountedCredential()
        DBlayer.open(encrypted = EncryptedConfig("https://api.example.com", KEY, PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("otp" to EncryptedSessionOperation("/api/auth/otp", false), "confirm" to EncryptedSessionOperation("/api/auth/confirm", false), "social" to EncryptedSessionOperation("/api/auth/social", false), "ticket" to EncryptedSessionOperation("/api/ticket", true))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel"), credentials = credentials, transport = DatabaseTransport(cable = { null }), modelsJson = modelsJSON, http = http)
        rejects<DbException.Http> { DBlayer.database.sessionCall("otp", OTP) }
        val issued = credentials.current
        val connection = CableConnection("/test", "token", { _, _ -> ClosingSocket() }, ManualTimers().timers)

        connection.open()
        eventually("the closed cable") { connection.state == CableConnectionState.Closed }

        assertEquals(issued to (1 to 0), credentials.current to (credentials.sets to credentials.clears))
    }
}
