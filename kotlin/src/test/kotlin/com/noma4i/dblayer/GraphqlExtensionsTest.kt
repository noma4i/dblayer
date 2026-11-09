package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class GraphqlExtensionsTest(private val queued: Boolean) {
    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "queued={0}")
        fun entries() = listOf(arrayOf(false), arrayOf(true))
    }
    @Test
    fun graphqlExtensionsReachDirectAndQueuedCalls() = bounded {
        for ((code, text, disposition) in listOf(
            Triple("DUPLICATE_CLIENT_KEY", null, null),
            Triple("UNKNOWN_PRODUCT", "FINISH", TransactionDisposition.Finish),
            Triple("REACTIVATION_GRANT_MISSING", "KEEP", TransactionDisposition.Keep),
            Triple("FUTURE_CODE", "LATER", TransactionDisposition.Unknown("LATER")),
        )) {
            val transport = MockTransport()
            val operation = if (code == "DUPLICATE_CLIENT_KEY") "RecordAlbumSend" else "PurchaseCreate"
            val root = if (code == "DUPLICATE_CLIENT_KEY") "recordAlbumSend" else "purchaseCreate"
            val database = openSigned(Config(null, null), modelsJSON.replace("CreateMoment", operation), transport)
            registerStandOperations(database, """[{"name":"$operation","document":"mutation $operation { $root { id } }"}]""")
            val extra = text?.let { """, "transactionDisposition":"$it"""" }.orEmpty()
            val response = """{"errors":[{"message":"refused","extensions":{"code":"$code"$extra,"detail":"refused","limit":3,"context":{"values":[true,null,2]}}},{"message":"secondary","extensions":{"code":"SECONDARY","transactionDisposition":"KEEP"}}]}"""
            transport.enqueue(operation, response)
            val extensions = withTimeout(5000) {
                if (queued) {
                    val receipt = database.send(CommandRequest(database.sessionEpoch(), "Moment", """{"text":"hello","media":null}""", "{}", null))
                    (database.outcome(receipt) as? WireOperationOutcome.Rejected ?: throw AssertionError("the refused purchase was not rejected")).failure.extensions
                } else {
                    (runCatching { database.call(database.sessionEpoch(), operation, "{}") }.exceptionOrNull() as? DbException.Graphql ?: throw AssertionError("the refused purchase did not throw Graphql")).extensions
                }
            }
            assertEquals(code, extensions?.code)
            assertEquals(disposition, extensions?.transactionDisposition)
            assertEquals(mapOf("detail" to "\"refused\"", "limit" to "3", "context" to """{"values":[true,null,2]}"""), extensions?.fields)
            val failure = OperationFailure("rejected", null, null, null, "refused", extensions)
            val restored = dbJson.decodeFromString(OperationFailure.serializer(), dbJson.encodeToString(OperationFailure.serializer(), failure))
            assertEquals(code, restored.extensions?.code)
            assertEquals(disposition, restored.extensions?.transactionDisposition)
            val encoded = dbJson.parseToJsonElement(dbJson.encodeToString(OperationFailure.serializer(), restored))
            assertEquals(mapOf("detail" to "\"refused\"", "limit" to "3", "context" to """{"values":[true,null,2]}"""), encoded.jsonObject["extensions"]?.jsonObject?.get("fields")?.jsonObject?.mapValues { it.value.jsonPrimitive.content })
        }
    }
}

class OperationFailureTest {
    @Test
    fun operationFailureCarriesEveryWireFailureField() {
        val wire = WireFailure("request_too_large", SessionCode.RATE_LIMITED, 9_000_000_000UL, 8_000_000_000UL, "too large", null)
        val failure = OperationFailure(wire.kind, wire.sessionCode, wire.requestSize, wire.requestLimit, wire.message, wire.extensions)
        assertEquals("request_too_large", failure.kind)
        assertEquals(SessionCode.RATE_LIMITED, failure.sessionCode)
        assertEquals(9_000_000_000UL, failure.requestSize)
        assertEquals(8_000_000_000UL, failure.requestLimit)
        assertEquals("too large", failure.message)
        assertEquals(failure, dbJson.decodeFromString(OperationFailure.serializer(), dbJson.encodeToString(OperationFailure.serializer(), failure)))
    }
}
