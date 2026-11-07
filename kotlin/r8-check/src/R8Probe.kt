package com.noma4i.dblayer

import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val schema = """[{"key":"Item","storage":"local","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"}}]}]"""
    OpenDatabase().open(Config(null, null, 32u), schema, OpenOptions("[]", false, false), OpenPorts(null, null, null, null)).use { db ->
        val epoch = db.sessionEpoch()
        val created = checkNotNull(db.create(epoch, "Item", """{"title":"kept"}""", "{}").optimisticValue)
        check(created.id.isNotEmpty())
        check(created.sync.state == WireSyncState.SYNCED)
        check(created.cells.contains(Cell.Text("kept")))
        val stream = db.subscribeValues(epoch, ReadRequest.List("""{"model":"Item"}"""))
        var snapshot: ReadValue.Snapshot? = null
        while (snapshot == null) {
            val value = checkNotNull(stream.pull()) { "the stream ended before its first snapshot" }
            if (value is ReadValue.Snapshot && value.rows.isNotEmpty()) snapshot = value
        }
        check(snapshot.rows.single().cells == created.cells)
        check(db.peekRow(epoch, "Item", created.id)?.cells == created.cells)
        stream.cancel()
        try {
            db.read(epoch, "Missing", "x", "title")
            error("Missing model must fail")
        } catch (_: DbException) {
        }
    }
    check(graphqlExtensions("{\"data\":{}}") == null)
    println("R8 native calls, structures, continuation callback, record, enum and error: passed")
}
