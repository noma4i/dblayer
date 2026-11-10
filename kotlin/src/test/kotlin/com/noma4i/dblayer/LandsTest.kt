package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class LandsTest {
    @Test
    fun lands_declaration_and_row_cross_the_bridge() = bounded {
        val schema = """[{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}}],"actions":[{"name":"create","root_shape":"upsert","mutation":"CreateRecord","root":"createRecord.record","has_optimistic":true,"has_temp_row_retention":true,"lands":[{"model":"Record","root":"createRecord.extra"}]}]}]"""
        val transport = MockTransport()
        transport.enqueue("CreateRecord", """{"data":{"createRecord":{"record":{"id":"r1"},"extra":{"id":"r2"}}}}""")
        val database = openSigned(Config(null, null), specs(schema), transport)
        registerStandOperations(database, """[{"name":"CreateRecord","document":"mutation CreateRecord { createRecord { record { id } extra { id } } }"}]""")
        val receipt = database.send(CommandRequest(database.sessionEpoch(), "Record", "{}", "{}", null))
        database.confirmed(receipt)
        assertEquals("r2", database.readRow(database.sessionEpoch(), "Record", "r2")?.id)
    }

}
