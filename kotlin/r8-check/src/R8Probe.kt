package com.noma4i.dblayer

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val schema = """[{"key":"Item","storage":"local","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"}}]}]"""
    OpenDatabase().open(Config(null, null, 32u), schema, OpenOptions("[]", null, false, false, true, true), null, null, null, null, null).use { db ->
        val available = Semaphore(0)
        val sink = object : ValueSink {
            override fun available() { available.release() }
        }
        val epoch = db.sessionEpoch()
        val created = checkNotNull(db.create(epoch, "Item", """{"title":"kept"}""", "{}").optimisticValue)
        check(created.id.isNotEmpty())
        check(created.sync.state == WireSyncState.SYNCED)
        check(created.cells.contains(Cell.Text("kept")))
        val stream = db.subscribe(epoch, StreamRequest.Query("""{"model":"Item"}"""), sink)
        var snapshot: StreamValue.Snapshot? = null
        while (snapshot == null) {
            check(available.tryAcquire(5, TimeUnit.SECONDS)) { "JNA callback was not delivered" }
            while (true) {
                val value = db.readStream(stream)?.value ?: break
                if (value is StreamValue.Snapshot && value.rows.isNotEmpty()) snapshot = value
            }
        }
        check(snapshot.rows.single().cells == created.cells)
        check(db.peekRow(epoch, "Item", created.id)?.cells == created.cells)
        db.cancelStream(stream)
        try {
            db.read(epoch, "Missing", "x", "title")
            error("Missing model must fail")
        } catch (_: DbException) {
        }
    }
    check(graphqlExtensions("{\"data\":{}}") == null)
    println("R8 native calls, structures, callback, record, enum and error: passed")
}
