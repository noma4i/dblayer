package com.noma4i.dblayer

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Test
import java.io.File

class MockContractTest {
    private val cases: JsonArray = Json.parseToJsonElement(File("../fixtures/mock-contract/pairs.json").readText()).jsonArray

    private fun author(name: String): Author = when (name) {
        "outbox" -> Author.OUTBOX
        "window" -> Author.WINDOW
        else -> error("the contract table has no author $name")
    }

    private fun replies(case: JsonObject): List<Pair<String, String>> = case.getValue("replies").jsonObject.flatMap { (operation, list) -> list.jsonArray.map { operation to it.toString() } }

    /** Every platform runs this table against the one server contract of the core: a repeat of a pair gets the first reply, a second body under one pair and an unscripted request are refused. */
    @Test
    fun mock_contract_pairs_table() = bounded {
        for (element in cases) {
            val case = element.jsonObject
            val mock = MockTransport()
            replies(case).forEach { (operation, reply) -> mock.enqueue(operation, reply) }
            val seen = case.getValue("calls").jsonArray.map { call ->
                val parts = call.jsonArray
                val request = TransportRequest("mutation ${parts[2].jsonPrimitive.content} { x }", parts[2].jsonPrimitive.content, parts[3].toString(), NetworkPurpose.STANDARD, author(parts[0].jsonPrimitive.content), parts[1].jsonPrimitive.content.toULong(), 1u)
                try { "data:${mock.mutate(request)}" } catch (error: DbException) { "refused" }
            }
            mock.claimUnscripted()
            assertEquals(case.getValue("name").jsonPrimitive.content, case.getValue("expect").jsonArray.map { it.jsonPrimitive.content }, seen)
        }
    }

    @Test
    fun server_contract_object_pairs_table() {
        for (element in cases) {
            val case = element.jsonObject
            val contract = ServerContract()
            replies(case).forEach { (operation, token) -> contract.script(operation, token) }
            val seen = case.getValue("calls").jsonArray.map { call ->
                val parts = call.jsonArray
                val operation = parts[2].jsonPrimitive.content
                val scope = parts[0].jsonPrimitive.content
                val wire = buildJsonObject {
                    put("requestId", parts[1].jsonPrimitive.content)
                    put("sequence", JsonNull)
                    put("body", buildJsonObject { put("operationName", operation); put("variables", parts[3]) })
                }.toString()
                when (val admitted = contract.admit(scope, wire)) {
                    is ContractAdmission.Stored -> "data:${admitted.token}"
                    is ContractAdmission.Refused, is ContractAdmission.TwoBodies -> "refused"
                    is ContractAdmission.Fresh -> contract.take(operation)?.also { contract.settle(scope, wire, it) }?.let { "data:$it" } ?: "refused"
                }
            }
            assertEquals(case.getValue("name").jsonPrimitive.content, case.getValue("expect").jsonArray.map { it.jsonPrimitive.content }, seen)
        }
    }
}
