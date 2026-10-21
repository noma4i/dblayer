package com.noma4i.dblayer

import com.noma4i.dblayer.models.StorageRow
import java.io.File
import java.nio.file.Files
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class StorageTest {
    @Test
    fun storageRulesAndWritesFollowTheSharedCases() = bounded {
        val cases = parseJson(File("../fixtures/storage.json").readText()) as JsonArray
        for (item in cases) {
            val fields = item.jsonObject
            val name = (fields.getValue("name") as JsonPrimitive).content
            val models = fields.getValue("models").toString()
            if (fields["error"] == JsonPrimitive(true)) {
                assertTrue(name, runCatching { Database.open(Config(null, null, 32u), models, null) }.exceptionOrNull() is DbException)
                continue
            }
            val directory = Files.createTempDirectory("dblayer-storage").toFile()
            try {
                val config = Config(File(directory, "db.sqlite").path, null, 32u)
                val transport = MockTransport()
                DBlayer.database = Database.open(config, models, transport)
                val state = if (name == "local") SyncState.Synced else SyncState.Pending
                val query = Query("StorageRow", StorageRow)
                assertEquals(emptyList<StorageRow>(), withTimeout(5000) { query.live.flow.first() })
                val row = StorageRow.create(record = "one")
                assertEquals(name, state, row.sync.state)
                assertEquals("one", withTimeout(5000) { query.live.flow.first { it.isNotEmpty() } }.first().record)
                val edited = row.update(record = "two")
                assertEquals(name, state, edited.sync.state)
                assertEquals("two", withTimeout(5000) { query.live.flow.first { it.firstOrNull()?.record == "two" } }.first().record)
                DBlayer.database.flush()
                assertEquals(name, emptyList<TransportRequest>(), transport.calls)
                DBlayer.database = Database.open(config, models, transport)
                val reopened = withTimeout(5000) { StorageRow.find(row.id).flow.first() }!!
                assertEquals(name, "two", reopened.record)
                assertEquals(name, state, reopened.sync.state)
                reopened.destroy()
                assertEquals(emptyList<StorageRow>(), withTimeout(5000) { query.live.flow.first { it.isEmpty() } })
                DBlayer.database.flush()
                assertEquals(name, emptyList<TransportRequest>(), transport.calls)
            } finally {
                directory.deleteRecursively()
            }
        }
    }
}
