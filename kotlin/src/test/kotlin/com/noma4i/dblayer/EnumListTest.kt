package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.ContainerShelf
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.builtins.ListSerializer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class EnumListTest {
    private suspend fun open() = openSignedOffline(Config(null), modelsJSON, """[{"name":"CreateContainer","document":"mutation CreateContainer(${'$'}title: String, ${'$'}shelves: [String!]) { createContainer(title: ${'$'}title, shelves: ${'$'}shelves) { id title shelves } }"}]""")

    @Test
    fun theTypedApiWritesAListOfEnumValuesAndAnAbsentListReadsAsNull() = bounded {
        open()
        assertEquals(listOf(ContainerShelf.Secondary), Container.create(title = "t", shelves = listOf(ContainerShelf.Secondary), account = AccountScope.current()).optimisticValue!!.shelves)
        assertNull(Container.create(title = "u", account = AccountScope.current()).optimisticValue!!.shelves)
    }

}
