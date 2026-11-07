package com.noma4i.dblayer

import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.mockwebserver.*
import org.junit.Assert.*
import org.junit.Test
import java.io.File
import java.security.KeyFactory
import java.security.interfaces.RSAPrivateCrtKey
import java.security.spec.*
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.CancellationException
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicInteger

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class BootstrapConfigTest {
    private val context get() = org.robolectric.RuntimeEnvironment.getApplication()

    @org.junit.Before fun onlineNetwork() {
        val manager = context.getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as android.net.ConnectivityManager
        val capabilities = org.robolectric.shadows.ShadowNetworkCapabilities.newInstance()
        org.robolectric.Shadows.shadowOf(capabilities).addCapability(android.net.NetworkCapabilities.NET_CAPABILITY_VALIDATED)
        org.robolectric.Shadows.shadowOf(manager).setNetworkCapabilities(manager.activeNetwork, capabilities)
    }
    @Test
    fun nativeHttpCancellationClosesTheCallDuringResponseBody() = bounded {
        val reading = CompletableDeferred<Unit>()
        val cancelled = CompletableDeferred<Unit>()
        val client = OkHttpClient.Builder().eventListener(object : EventListener() {
            override fun responseBodyStart(call: Call) { reading.complete(Unit) }
            override fun canceled(call: Call) { cancelled.complete(Unit) }
        }).build()
        val server = MockWebServer()
        server.enqueue(MockResponse().setBody("pending").throttleBody(1, 60, java.util.concurrent.TimeUnit.SECONDS))
        server.start()
        val http = OkHttpEncryptedHttp(server.url("/").toString().trimEnd('/'), client)
        val task = launch { runCatching { http.post("/probe", "x", NetworkPurpose.STANDARD) } }
        try {
            kotlinx.coroutines.withTimeout(5000) { reading.await() }
            task.cancel()
            assertTrue("native call remained active after coroutine cancellation", kotlinx.coroutines.withTimeoutOrNull(2000) { cancelled.await(); true } == true)
            task.join()
        } finally {
            client.dispatcher.cancelAll()
            server.shutdown()
            task.cancelAndJoin()
        }
    }

    @Test
    fun coroutineCancellationReachesTheCoreAndCancelsForeignHttp() = bounded {
        val started = CompletableDeferred<Unit>()
        val stopped = CompletableDeferred<Throwable>()
        val http = object : EncryptedHttp {
            override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply {
                started.complete(Unit)
                try {
                    awaitCancellation()
                } catch (error: Throwable) {
                    stopped.complete(error)
                    throw error
                }
            }
        }
        val call = launch { DBlayer.serverConfig(context, encrypted(), AppPlatform.ANDROID, "1.2", http = http) }
        started.await()
        call.cancelAndJoin()
        assertTrue(stopped.await() is CancellationException)
    }

    @Test
    fun bootstrapTimeoutWaitsForCancellationAndReportsTheAttempt() = bounded {
        val calls = AtomicInteger()
        val retries = RetryProgress()
        val http = object : EncryptedHttp {
            override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply {
                calls.incrementAndGet()
                throw classifyEncryptedHttpFailure(SocketTimeoutException("timeout"))
            }
        }
        val connected = async { retries.flow.take(2).toList() }
        val call = launch { DBlayer.serverConfig(context, encrypted(), AppPlatform.ANDROID, "1.2", http = http, retries = retries) }
        val (first, second) = connected.await()
        assertNotEquals("the reader sees the current snapshot once", first, second)
        val attempt = retries.flow.first { it.activeCalls == 1u }.lastAttempt!!
        assertEquals(1uL, attempt.attempt)
        assertTrue(attempt.delayMs in 800uL..1000uL)
        assertEquals("network", attempt.reason)
        call.cancelAndJoin()
        assertEquals(RetrySnapshot(0u, null), retries.flow.first { it.activeCalls == 0u })
        assertEquals(1, calls.get())
    }

    private val kat = Json.parseToJsonElement(File("../fixtures/bootstrap-config/bootstrap_kat.json").readText()).jsonObject
    private fun JsonObject.text(key: String) = getValue(key).jsonPrimitive.content
    private fun decode(text: String) = Base64.getUrlDecoder().decode(text)
    private fun encode(bytes: ByteArray) = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
    private fun der(tag: Int, bytes: ByteArray): ByteArray = byteArrayOf(tag.toByte(), 0x82.toByte(), (bytes.size shr 8).toByte(), bytes.size.toByte()) + bytes
    private val privateKey: RSAPrivateCrtKey by lazy {
        val pkcs1 = Base64.getDecoder().decode(kat.text("rsa_private_pem").lines().filter { !it.startsWith("-") }.joinToString(""))
        val algorithm = byteArrayOf(0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86.toByte(), 0x48, 0x86.toByte(), 0xf7.toByte(), 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00)
        KeyFactory.getInstance("RSA").generatePrivate(PKCS8EncodedKeySpec(der(0x30, byteArrayOf(2, 1, 0) + algorithm + der(4, pkcs1)))) as RSAPrivateCrtKey
    }
    private fun encrypted(origin: String = kat.text("audience")): EncryptedConfig {
        val publicKey = KeyFactory.getInstance("RSA").generatePublic(RSAPublicKeySpec(privateKey.modulus, privateKey.publicExponent))
        return EncryptedConfig(origin, Base64.getEncoder().encodeToString(publicKey.encoded), kat.text("kid"), EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", kat.text("path"), mapOf("otp" to EncryptedSessionOperation("/api/auth/otp", EncryptedOperationClass.AUTHENTICATED), "confirm" to EncryptedSessionOperation("/api/auth/confirm", EncryptedOperationClass.AUTHENTICATED), "social" to EncryptedSessionOperation("/api/auth/social", EncryptedOperationClass.AUTHENTICATED), "ticket" to EncryptedSessionOperation("/api/ticket", EncryptedOperationClass.SESSIONLESS))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
    }
    private fun open(token: String): JsonObject {
        val parts = token.split(".")
        val rsa = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding").apply { init(Cipher.DECRYPT_MODE, privateKey) }
        val aes = Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(Cipher.DECRYPT_MODE, SecretKeySpec(rsa.doFinal(decode(parts[1])), "AES"), GCMParameterSpec(128, decode(parts[2])))
            updateAAD(parts[0].toByteArray())
        }
        return Json.parseToJsonElement(String(aes.doFinal(decode(parts[3]) + decode(parts[4])))).jsonObject
    }
    private fun answer(token: String): String {
        val plain = open(token)
        val body = plain.getValue("body").jsonObject
        assertEquals(setOf("audience", "kid", "method", "path", "requestId", "issuedAt", "responseKey", "body"), plain.keys)
        assertTrue(body.text("platform") in listOf("ios", "android"))
        val issued = plain.getValue("issuedAt").jsonPrimitive.long
        assertTrue(System.currentTimeMillis() / 1000 - issued in -30..120)
        val signed = JsonObject(plain.filterKeys { it != "issuedAt" }.toSortedMap()).toString()
        val digest = java.security.MessageDigest.getInstance("SHA-256").digest(signed.toByteArray()).joinToString("") { "%02x".format(it) }
        val config = Json.parseToJsonElement(kat.getValue("response").jsonObject.text("plaintext")).jsonObject.getValue("body").jsonObject
        val response = buildJsonObject {
            put("requestId", plain.getValue("requestId"))
            put("requestDigest", digest)
            put("sequence", JsonNull)
            put("body", buildJsonObject {
                config.forEach { (key, value) -> put(key, value) }
                put("updateRequired", true)
                put("future", true)
            })
        }
        val header = encode("""{"alg":"dir","enc":"A256GCM"}""".toByteArray())
        val iv = ByteArray(12) { 7 }
        val aes = Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(Cipher.ENCRYPT_MODE, SecretKeySpec(decode(plain.text("responseKey")), "AES"), GCMParameterSpec(128, iv))
            updateAAD(header.toByteArray())
        }
        val sealed = aes.doFinal(response.toString().toByteArray())
        return "$header..${encode(iv)}.${encode(sealed.dropLast(16).toByteArray())}.${encode(sealed.takeLast(16).toByteArray())}"
    }

    @Test
    fun configBeforeDatabaseUsesTheCoreAndKeepsTheClassOfEveryOutcomeTheWrapperTypes() = bounded {
        for (platform in listOf(AppPlatform.IOS, AppPlatform.ANDROID)) {
            for (kind in listOf("ok", "http", "pin")) {
                var calls = 0
                val http = object : EncryptedHttp {
                    override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply {
                        calls++
                        assertEquals(kat.text("path"), path)
                        return if (kind == "http") EncryptedReply(400u, "") else EncryptedReply(200u, answer(body))
                    }
                }
                val config = encrypted().let { if (kind == "pin") it.copy(pin = "wrong") else it }
                try {
                    val value = DBlayer.serverConfig(context, config, platform, "3.8.0", "120", http)
                    assertEquals("ok", kind)
                    assertEquals(1u, value.configVersion)
                    assertEquals("3.7.2", value.minAppVersion)
                    assertTrue(value.updateRequired && value.trackingPromptEnabled)
                    assertEquals("{}", value.flagsJson)
                    assertTrue(value.storeUrl!!.contains("apps.apple.com"))
                } catch (error: DbException) {
                    when (kind) {
                        "pin" -> assertEquals(SecurityCode.SERVER_KEY_MISMATCH, (error as DbException.Security).code)
                        "http" -> assertEquals(400.toUShort(), (error as DbException.Http).status)
                        else -> throw error
                    }
                    assertSame(error, classifyEncryptedHttpFailure(error))
                }
                assertEquals(if (kind == "pin") 0 else 1, calls)
            }
        }
    }

    @Test
    fun actualHttpKeepsTheAppClientAndRefusesRedirects() = bounded {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(302).setHeader("Location", "/redirect"))
            server.enqueue(MockResponse().setBody("followed"))
            server.start()
            val polluted = OkHttpClient.Builder().addInterceptor { chain ->
                chain.proceed(chain.request().newBuilder().header("X-App", "kept").build())
            }.build()
            val reply = OkHttpEncryptedHttp(server.url("/").toString().trimEnd('/'), polluted).post("/api/bootstrap", "jwe", NetworkPurpose.STANDARD)
            assertEquals(302.toUShort(), reply.status)
            assertEquals(1, server.requestCount)
            val request = server.takeRequest()
            assertEquals(listOf(userAgent(null)), request.headers.values("User-Agent"))
            assertEquals("POST /api/bootstrap", "${request.method} ${request.path}")
            val applicationHeaders = request.headers.names().filter { it.lowercase() !in listOf("host", "content-length", "transfer-encoding", "connection", "user-agent", "accept-encoding") }
            assertEquals(setOf("X-App", "Content-Type"), applicationHeaders.toSet())
            assertEquals("application/jose", request.getHeader("Content-Type"))
            assertEquals("jwe", request.body.readUtf8())
        }
    }
}
