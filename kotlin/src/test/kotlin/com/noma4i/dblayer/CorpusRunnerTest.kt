package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import java.io.File
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private fun observation(kind: String, value: JsonElement? = null): JsonObject =
    JsonObject(if (value == null) mapOf("o" to JsonPrimitive(kind)) else mapOf("o" to JsonPrimitive(kind), "v" to value))

private fun failure(kind: String): List<JsonObject> = listOf(observation("error", JsonObject(mapOf("kind" to JsonPrimitive(kind)))))

/** Runs the inputs of `specs/machines/corpus.md` (database runner) through the public API of `Database`. `open`, `create`, `has`, `set_online`, `close` run; every other input answers `unsupported`. */
private class DatabaseRunner {
    var database: Database? = null
    val rows = HashMap<String, String>()

    suspend fun apply(input: JsonObject): List<JsonObject> {
        val name = input["in"]?.let { (it as JsonPrimitive).contentOrNull } ?: ""
        val args = (input["args"] as? JsonObject) ?: JsonObject(emptyMap())
        if (name == "open") {
            return try {
                val opened = openDatabase(Config(null, null, 32u), modelsJSON, MockTransport())
                opened.setOnline(false)
                database = opened
                listOf(observation("opened"))
            } catch (error: Exception) { failure("open") }
        }
        val db = database ?: return failure("not_open")
        return when (name) {
            "set_online" -> {
                val online = (args["online"] as? JsonPrimitive)?.booleanOrNull ?: false
                try { db.setOnline(online); listOf(observation("online", JsonPrimitive(online))) } catch (error: Exception) { failure("online") }
            }
            "create" -> {
                val row = (args["as"] as? JsonPrimitive)?.contentOrNull
                val text = (args["text"] as? JsonPrimitive)?.contentOrNull
                if (row == null || text == null) return failure("arguments")
                try {
                    val receipt = db.send(CommandRequest(db.sessionEpoch(), "Moment", JsonObject(mapOf("text" to JsonPrimitive(text))).toString(), "{}", null))
                    val created = receipt.optimisticValue ?: return failure("no_row")
                    rows[row] = created.id
                    listOf(observation("created", JsonObject(mapOf("row" to JsonPrimitive(row)))))
                } catch (error: Exception) { failure("create") }
            }
            "has" -> {
                val row = (args["row"] as? JsonPrimitive)?.contentOrNull ?: return failure("arguments")
                try {
                    val found = db.readQuery(db.sessionEpoch(), """{"model":"Moment"}""")
                    val id = rows[row] ?: row
                    listOf(observation("row", JsonObject(mapOf("row" to JsonPrimitive(row), "present" to JsonPrimitive(found.any { it.id == id })))))
                } catch (error: Exception) { failure("has") }
            }
            "close" -> try { db.closeDatabase(); database = null; listOf(observation("closed")) } catch (error: Exception) { failure("close") }
            else -> listOf(observation("unsupported", JsonPrimitive(name)))
        }
    }
}

class CorpusRunnerTest {
    @Test
    fun theSharedCorpusIsGreenThroughTheKotlinWrapper() = bounded {
        val files = File("../core/tests/corpus").listFiles { file -> file.extension == "jsonl" }!!.sortedBy { it.name }
        assertTrue(files.isNotEmpty())
        for (file in files) {
            val runner = DatabaseRunner()
            val lines = file.readLines().filter { it.isNotBlank() }.drop(1)
            for ((index, line) in lines.withIndex()) {
                val step = Json.parseToJsonElement(line).jsonObject
                val expected = step["out"] ?: JsonArray(emptyList())
                val got = JsonArray(runner.apply(step))
                assertEquals("${file.name} step ${index + 1} input ${step["in"]}", expected, got)
            }
        }
    }
}
