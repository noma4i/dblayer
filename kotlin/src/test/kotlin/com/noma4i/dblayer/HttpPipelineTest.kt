package com.noma4i.dblayer

import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

@Serializable
private data class Probe(val id: String)

private object ProbeQuery : GraphQLQuery<Probe, Unit> {
    override val operationName = "Probe"
    override val document = "query Probe { probe { id } }"
    override val dataSerializer = Probe.serializer()
    override val variablesSerializer = Unit.serializer()
}

private object ProbeMutation : GraphQLMutation<Probe, Unit> {
    override val operationName = "Probe"
    override val document = "mutation Probe { probe { id } }"
    override val dataSerializer = Probe.serializer()
    override val variablesSerializer = Unit.serializer()
}

class HttpPipelineTest {
    private val server = MockWebServer()
    private val signer = RequestSigner("secret")

    @Before fun start() = server.start()
    @After fun stop() = server.shutdown()

    private fun makeRun(onFailure: (RequestFailure) -> Unit = {}) = OperationRun(
        GraphQLHttpTransport(server.url("/graphql").toString(), OkHttpClient(), signer, ServerClock(), "1.0", "1", "device-1") { "token" },
        PersistedQueryHashes(),
        onFailure,
    )

    private fun transport(run: OperationRun = makeRun()): GraphQLTransport = NetworkTransport(run, CableClient("/cable", "token", { _, _ -> TestCableSocket(emptyList()) }))

    private fun reply(status: Int, body: String) = MockResponse().setResponseCode(status).setBody(body)

    private fun sentBody(): JsonObject {
        val request = server.takeRequest()
        val wire = dbJson.decodeFromString(EnvelopeWire.serializer(), request.body.readUtf8())
        return parseJson(String(signer.payloadCipher().open(PayloadEnvelope(wire.v, wire.iv, wire.ct, wire.tag)))) as JsonObject
    }

    @Test
    fun queryDecodesDataAndNullData() = bounded {
        server.enqueue(reply(200, """{"data":{"id":"7"}}"""))
        server.enqueue(reply(200, """{"data":null}"""))
        assertEquals("7", transport().query(ProbeQuery, Unit).data?.id)
        assertNull(transport().query(ProbeQuery, Unit).data)
    }

    @Test
    fun queryReportsGraphQLErrorsFromEveryStatus() = bounded {
        for (status in listOf(200, 201, 400, 403, 422)) {
            server.enqueue(reply(status, """{"errors":[{"message":"Rejected","extensions":{"code":"REGISTRATION_REQUIRED"}}]}"""))
            val error = assertThrows(GraphQLTransportError::class.java) { bounded { transport().query(ProbeQuery, Unit) } }
            assertEquals(GraphQLTransportError.GraphQL(listOf("Rejected"), GraphqlExtensions("REGISTRATION_REQUIRED", null)), error)
        }
    }

    @Test
    fun mutationReturnsDataAndErrors() = bounded {
        server.enqueue(reply(200, """{"data":{"id":"7"},"errors":[{"message":"Rejected","extensions":{"code":"BAD_INPUT"}}]}"""))
        val response = transport().mutate(ProbeMutation, Unit)
        assertEquals("7", response.data?.id)
        assertEquals(listOf("Rejected"), response.errors.map { it.message })
        assertEquals(listOf("BAD_INPUT"), response.errors.map { it.code })
        server.enqueue(reply(401, """{"data":null,"errors":[{"message":"Rejected"}]}"""))
        assertEquals(GraphQLTransportError.Unauthenticated, assertThrows(GraphQLTransportError::class.java) { bounded { transport().mutate(ProbeMutation, Unit) } })
    }

    @Test
    fun requestIsSignedEncryptedAndIdentifiesThePersistedQuery() = bounded {
        server.enqueue(reply(200, """{"data":{"id":"7"}}"""))
        transport().query(ProbeQuery, Unit)
        val request = server.takeRequest()
        assertEquals("1", request.getHeader("X-Payload-Encryption"))
        assertEquals("2", request.getHeader("X-Request-Signature-Version"))
        assertEquals("Bearer token", request.getHeader("authorization"))
        assertEquals("android", request.getHeader("x-app-platform"))
        assertTrue(request.getHeader("X-Request-Signature")!!.length == 64)
        val wire = dbJson.decodeFromString(EnvelopeWire.serializer(), request.body.readUtf8())
        val body = parseJson(String(signer.payloadCipher().open(PayloadEnvelope(wire.v, wire.iv, wire.ct, wire.tag)))) as JsonObject
        assertEquals(JsonPrimitive("Probe"), body["operationName"])
        assertNull(body["query"])
        assertNotNull(body["extensions"])
    }

    @Test
    fun tokenSupplierReadsTheCurrentTokenOncePerRequest() = bounded {
        val tokens = listOf("token-1", "token-2")
        var reads = 0
        val run = OperationRun(
            GraphQLHttpTransport.make(
                server.url("/graphql").toString(),
                TransportInputs("1.0", "1", "device-1", tokenSupplier = { tokens[reads++] }),
                OkHttpClient(),
                signer,
                ServerClock(),
            ),
            PersistedQueryHashes(),
        )
        server.enqueue(reply(200, """{"data":{"id":"7"}}"""))
        server.enqueue(reply(200, """{"data":{"id":"8"}}"""))

        val network = transport(run)
        network.query(ProbeQuery, Unit)
        network.query(ProbeQuery, Unit)

        assertEquals(2, reads)
        assertEquals("Bearer token-1", server.takeRequest().getHeader("authorization"))
        assertEquals("Bearer token-2", server.takeRequest().getHeader("authorization"))
    }

    @Test
    fun unknownPersistedQueryIsSentAgainWithTheDocument() = bounded {
        server.enqueue(reply(200, """{"errors":[{"message":"PersistedQueryNotFound"}]}"""))
        server.enqueue(reply(200, """{"data":{"id":"7"}}"""))
        assertEquals("7", transport().query(ProbeQuery, Unit).data?.id)
        assertNull(sentBody()["query"])
        assertEquals(JsonPrimitive(ProbeQuery.document), sentBody()["query"])
    }

    @Test
    fun encryptedResponseIsOpened() = bounded {
        val sealed = signer.payloadCipher().seal("""{"data":{"id":"9"}}""".toByteArray())
        val wire = dbJson.encodeToString(EnvelopeWire.serializer(), EnvelopeWire(sealed.envelopeVersion, sealed.iv, sealed.ciphertext, sealed.tag))
        server.enqueue(reply(200, wire).setHeader("x-payload-encryption", "1"))
        assertEquals("9", transport().query(ProbeQuery, Unit).data?.id)
    }

    @Test
    fun failuresAreReportedOnce() = bounded {
        val failures = mutableListOf<RequestFailure>()
        server.enqueue(reply(502, ""))
        assertThrows(GraphQLTransportError::class.java) { bounded { transport(makeRun { failures.add(it) }).query(ProbeQuery, Unit) } }
        assertEquals(listOf(RequestFailure("Probe", 502, "server")), failures)
    }
}
