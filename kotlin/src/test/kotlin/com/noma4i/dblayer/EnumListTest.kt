package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.ContainerShelf
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.builtins.ListSerializer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class EnumListTest {
    private suspend fun open() = reopenSigned(Config(null, null, 32u), modelsJSON)

    @Test
    fun aListOfEnumValuesIsStoredReadBackAndKeepsAValueTheDeclarationLacks() = bounded {
        val database = open()
        val wire = database.send(CommandRequest(database.sessionEpoch(), "Container", """{"title":"t","shelves":["PRIMARY","FUTURE"]}""", "{}", null)).row
        val container = Container.read(wire)
        assertEquals(listOf(ContainerShelf.Primary, ContainerShelf.Unknown("FUTURE")), container.shelves)
    }

    @Test
    fun theTypedApiWritesAListOfEnumValuesAndAnAbsentListReadsAsNull() = bounded {
        open()
        assertEquals(listOf(ContainerShelf.Secondary), Container.send(AccountScope.current(), title = "t", shelves = listOf(ContainerShelf.Secondary)).row.shelves)
        assertNull(Container.send(AccountScope.current(), title = "u").row.shelves)
    }

    @Test
    fun aListOfEnumValuesEncodesAsTheirTexts() {
        val serializer = ListSerializer(ContainerShelf.serializer())
        assertEquals("""["PRIMARY","x"]""", dbJson.encodeToString(serializer, listOf(ContainerShelf.Primary, ContainerShelf.Unknown("x"))))
    }

}
