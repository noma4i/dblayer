package com.noma4i.dblayer

import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Test

private const val CLOUD_MODELS = """[{"key":"Note","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"},"optional":true}],"storage":"local","cloud":["title"]}]"""

private suspend fun titles(database: Database): Map<String, String> {
    val live = database.liveQuery("""{"model":"Note"}""", object : QueryListener {
        override fun changed(change: LiveChange) = Unit
    })
    return live.current().rows.associate { row -> row.id to (row.cells.filterIsInstance<Cell.Text>().first().value) }
}

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class DriveConvergenceTest {
    @Test
    fun twoDatabasesConvergeThroughTheAdapterOnASharedDrive() = bounded {
        val drive = FakeDrive()
        val server = MockWebServer().apply { dispatcher = drive; start() }
        val token = DriveToken { "token" }
        fun cloud() = DriveCloud(DriveApi(OkHttpClient(), token, server.url("/")), token)
        val first = openDatabase(Config(null, null, 32u), CLOUD_MODELS, null)
        val second = openDatabase(Config(null, null, 32u), CLOUD_MODELS, null)
        val firstLink = first.connectCloud(cloud())
        val secondLink = second.connectCloud(cloud())
        val id = first.create("Note", """{"title":"hello"}""", "{}").id
        firstLink.syncNow()
        secondLink.syncNow()
        assertEquals(mapOf(id to "hello"), titles(second))
        second.update("Note", id, """{"title":"edited"}""", "{}")
        secondLink.syncNow()
        firstLink.syncNow()
        assertEquals(mapOf(id to "edited"), titles(first))
        firstLink.stop()
        secondLink.stop()
        server.shutdown()
    }
}
