package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Test

class LandsTest {
    @Test
    fun lands_declaration_and_row_cross_the_bridge() = bounded {
        val schema = """[{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}}],"actions":[{"name":"create","root_shape":"upsert","durable":true,"mutation":"CreateRecord","root":"createRecord.record","has_optimistic":true,"has_temp_row_retention":true,"lands":[{"model":"Record","root":"createRecord.extra"}]}]}]"""
        val transport = MockTransport()
        transport.enqueue("CreateRecord", """{"data":{"createRecord":{"record":{"id":"r1"},"extra":{"id":"r2"}}}}""")
        val database = openDatabase(Config(null, null, 64u), schema, transport)
        database.registerOperations("""[{"name":"CreateRecord","document":"mutation CreateRecord { createRecord { record { id } extra { id } } }"}]""")
        val row = database.send(CommandRequest(database.sessionEpoch(), "Record", "{}", "{}", null)).row
        database.settled("Record", row.id, null)
        database.warm("""{"model":"Record"}""")
        assertEquals("r2", database.peekRow("Record", "r2")?.id)
    }

}
