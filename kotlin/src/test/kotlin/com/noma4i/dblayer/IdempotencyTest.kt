package com.noma4i.dblayer

import java.io.File
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test

class IdempotencyTest {
    @Test
    fun aQueuedMutationKeepsItsKeyAcrossARefusalAndUsesAnExplicitKey() = bounded {
        val fixture = parseJson(File("../fixtures/idempotency.json").readText()).jsonObject
        for (explicit in listOf(false, true)) {
            val transport = MockTransport()
            transport.enqueue("RecordSend", fixture.getValue("refusal").toString())
            transport.enqueue("RecordSend", fixture.getValue("answer").toString())
            val database = Database.open(Config(null, null, 32u), fixture.getValue("models").toString(), transport)
            database.registerOperations(fixture.getValue("operations").toString())
            val row = database.create("Record", """{"text":"hello"}""", if (explicit) """{"clientMutationId":"app-key"}""" else "{}")
            database.flush()
            database.retry("Record", row.id)
            val calls = transport.calls.filter { it.operationName == "RecordSend" }
            assertEquals(2, calls.size)
            val key = parseJson(calls[0].variablesJson).jsonObject.getValue("input").jsonObject["clientMutationId"]?.jsonPrimitive?.content
            assertNotNull(key)
            if (explicit) assertEquals("app-key", key) else assertEquals(32, key!!.length)
            assertEquals(parseJson(calls[0].variablesJson), parseJson(calls[1].variablesJson))
            database.close()
        }
    }
}
