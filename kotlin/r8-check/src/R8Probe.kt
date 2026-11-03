package com.noma4i.dblayer

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val schema = """[{"key":"Item","storage":"local","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"}}]}]"""
    OpenDatabase().open(Config(null, null, 32u), schema, OpenOptions("[]", null, false, false, true, true), null, null, null, null, null).use { db ->
        val delivered = CountDownLatch(1)
        val received = AtomicReference<StreamDelivery>()
        val sink = object : ValueSink {
            override fun delivered(delivery: StreamDelivery) {
                if (delivery.value is StreamValue.Snapshot && (delivery.value as StreamValue.Snapshot).rows.isNotEmpty()) {
                    received.set(delivery)
                    delivered.countDown()
                }
            }
        }
        val created = db.create("Item", """{"title":"kept"}""", "{}")
        check(created.id.isNotEmpty())
        check(created.sync.state == WireSyncState.SYNCED)
        check(created.cells.contains(Cell.Text("kept")))
        val stream = db.subscribe(db.sessionEpoch(), StreamRequest.Query("""{"model":"Item"}"""), sink)
        check(delivered.await(5, TimeUnit.SECONDS)) { "JNA callback was not delivered" }
        check((received.get().value as StreamValue.Snapshot).rows.single().cells == created.cells)
        check(db.peekRow("Item", created.id)?.cells == created.cells)
        db.cancelStream(stream)
        try {
            db.read("Missing", "x", "title")
            error("Missing model must fail")
        } catch (_: DbException) {
        }
    }
    check(graphqlExtensions("{\"data\":{}}") == null)
    println("R8 native calls, structures, callback, record, enum and error: passed")
}
