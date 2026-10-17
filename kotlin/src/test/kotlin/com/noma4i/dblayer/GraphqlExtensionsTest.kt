package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
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
            val operation = if (code == "DUPLICATE_CLIENT_KEY") "MessageAlbumSend" else "PurchaseCreate"
            val root = if (code == "DUPLICATE_CLIENT_KEY") "messageAlbumSend" else "purchaseCreate"
            val database = Database.open(Config(null, null, 32u), modelsJSON.replace("CreateMoment", operation), transport)
            database.registerOperations("""[{"name":"$operation","document":"mutation $operation { $root { id } }"}]""")
            val extra = text?.let { """, "transactionDisposition":"$it"""" }.orEmpty()
            val response = """{"errors":[{"message":"a message with this client key already exists in the chat","extensions":{"code":"$code"$extra}},{"message":"secondary","extensions":{"code":"SECONDARY","transactionDisposition":"KEEP"}}]}"""
            transport.enqueue(operation, response)
            val error = runCatching {
                withTimeout(5000) {
                    if (queued) {
                        val row = database.create("Moment", """{"text":"hello","media":null}""", "{}")
                        database.settled("Moment", row.id)
                    } else database.run(operation, "{}")
                }
            }.exceptionOrNull() as? DbException.Graphql ?: throw AssertionError("the refused purchase did not throw Graphql")
            assertEquals(code, error.extensions?.code)
            assertEquals(disposition, error.extensions?.transactionDisposition)
            val failure = OperationFailure("rejected", "refused", error.extensions)
            val restored = dbJson.decodeFromString(OperationFailure.serializer(), dbJson.encodeToString(OperationFailure.serializer(), failure))
            assertEquals(code, restored.extensions?.code)
            assertEquals(disposition, restored.extensions?.transactionDisposition)
            database.close()
        }
    }
}
