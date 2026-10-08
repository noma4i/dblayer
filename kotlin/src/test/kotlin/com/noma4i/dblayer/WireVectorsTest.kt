package com.noma4i.dblayer

import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class WireVectorsTest {
    private fun signature(variables: String, nonce: String) = RequestSigner("test_secret").sign(
        SignatureInput("TestOperation", variables.toByteArray(), "test_bearer_token", "9B3D2C1A-0000-4000-8000-000000000001", 1_700_000_000.0, nonce),
    )

    @Test
    fun signatureMatchesServerVector() {
        val headers = signature("{}", "00112233445566778899aabbccddeeff")
        assertEquals("2652e11a3ed0441719e4c732124e5871e8bfa668699c9392e7ec6e6531213452", headers.signature)
        assertEquals("1700000000", headers.timestamp)
        assertEquals("2", headers.version)
    }

    @Test
    fun variablesAndNonceChangeSignature() {
        val base = signature("{}", "00112233445566778899aabbccddeeff")
        assertNotEquals(base.signature, signature("{\"a\":1}", base.nonce).signature)
        assertNotEquals(base.signature, signature("{}", "ffeeddccbbaa99887766554433221100").signature)
        assertEquals(32, RequestSigner.makeNonce().length)
    }

    @Test
    fun sealWithFixedIvMatchesServerVector() {
        val cipher = PayloadCipher("test-secret")
        val vector = ByteArray(12) { it.toByte() }
        val envelope = cipher.seal("""{"a":1}""".toByteArray(), vector)
        assertArrayEquals(byteArrayOf(-91, -11, -57, 42, 56, -9, 88), envelope.ciphertextBytes)
        assertArrayEquals(byteArrayOf(125, -23, -95, 19, -33, -128, 121, -36, 1, 49, 120, -34, 65, 50, -105, -89), envelope.tagBytes)
        assertEquals("""{"a":1}""", String(cipher.open(envelope)))
    }

    @Test
    fun openRejectsTamperedAndUnsupportedEnvelopes() {
        val cipher = PayloadCipher("test-secret")
        val envelope = cipher.seal("""{"a":1}""".toByteArray(), ByteArray(12) { it.toByte() })
        val changed = envelope.copy(tag = envelope.tag.dropLast(2) + "a8")
        assertEquals(PayloadCipher.Failure.AuthenticationFailed, assertThrows(PayloadCipher.CipherException::class.java) { cipher.open(changed) }.failure)
        assertEquals(PayloadCipher.Failure.UnsupportedVersion, assertThrows(PayloadCipher.CipherException::class.java) { cipher.open(envelope.copy(envelopeVersion = 2)) }.failure)
    }

    @Test
    fun canonicalJsonSortsEscapesAndPrintsIntegers() {
        fun text(value: kotlinx.serialization.json.JsonElement) = String(CanonicalJson.encode(value))
        assertEquals("""{"A":2,"a":3,"b":1}""", text(buildJsonObject { put("b", 1); put("A", 2); put("a", 3) }))
        assertEquals("""{"list":[{"y":null,"z":true},"second"]}""", text(buildJsonObject {
            put("list", buildJsonArray { add(buildJsonObject { put("z", true); put("y", JsonNull) }); add(JsonPrimitive("second")) })
        }))
        assertEquals("\"Tom \\u0026 Jerry \\u003cb\\u003e\\u2028\\n\"", text(JsonPrimitive("Tom & Jerry <b> \n")))
        assertEquals("1", text(JsonPrimitive(1.0)))
        assertEquals("1.5", text(JsonPrimitive(1.5)))
        assertEquals("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", sha256(CanonicalJson.encode(buildJsonObject { })).hex())
    }

    @Test
    fun retryPolicyAllowsEachReasonOnceAndNeverReplaysMutations() {
        val policy = TransportRetryPolicy()
        assertTrue(policy.shouldRetry(TransportRetryReason.PersistedQueryNotFound, GraphQLOperationKind.Query))
        assertFalse(policy.shouldRetry(TransportRetryReason.PersistedQueryNotFound, GraphQLOperationKind.Query))
        assertFalse(TransportRetryPolicy().shouldRetry(TransportRetryReason.SignatureReplay, GraphQLOperationKind.Mutation))
        assertEquals(TransportRetryReason.SignatureExpired, TransportRetryPolicy.reason("x", "SIGNATURE_EXPIRED"))
        assertNull(TransportRetryPolicy.reason("x", null))
    }

    @Test
    fun statusAndMessagesClassifyTransportErrors() {
        assertEquals(GraphQLTransportError.Server(502), GraphQLTransportError.classify(502, emptyList()))
        assertEquals(GraphQLTransportError.Unauthenticated, GraphQLTransportError.classify(401, emptyList()))
        assertEquals(GraphQLTransportError.GraphQL(listOf("no")), GraphQLTransportError.classify(200, listOf("no")))
        assertNull(GraphQLTransportError.classify(200, emptyList()))
    }

    @Test
    fun responseBodyReadsCodesAndStatuses() {
        val body = """{"data":null,"errors":[{"message":"m","extensions":{"code":"C","http":{"status":429}}}]}"""
        val parsed = GraphQLResponseBody.parse(body.toByteArray())!!
        assertEquals(listOf(GraphQLResponseError("m", "C", 429)), parsed.errors)
        assertNull(GraphQLResponseBody.parse("[1]".toByteArray()))
    }
}
