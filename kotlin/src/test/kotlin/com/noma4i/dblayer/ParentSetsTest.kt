package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.Record
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class ParentSetsTest {
    @Test
    fun aLocalRecordMovesTheContainerPointerAndDiscardingItGivesThePointerBack() = bounded {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val container = Container.create(title = "t")
        val record = Record.create(containerId = container.id, authorId = "u1", text = "hi")

        val pointed = withTimeout(5000) { Container.find(container.id).current() }
        assertEquals(record.id, pointed?.lastRecordId)
        assertNotNull(pointed?.lastRecordAt)

        record.discard()
        val restored = withTimeout(5000) { Container.find(container.id).current() }
        assertNull(restored?.lastRecordId)
        assertNull(restored?.lastRecordAt)
    }
}
