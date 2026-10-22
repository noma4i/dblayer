package com.noma4i.dblayer

import com.noma4i.dblayer.models.Container
import com.noma4i.dblayer.models.ContainerShelf
import com.noma4i.dblayer.models.ContainerShelfKind
import com.noma4i.dblayer.models.Inbox
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.builtins.ListSerializer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Test

class EnumListTest {
    private suspend fun open() = Database.open(Config(null, null, 32u), modelsJSON, null).also { DBlayer.database = it }

    @Test
    fun aListOfEnumValuesIsStoredReadBackAndKeepsAValueTheDeclarationLacks() = bounded {
        val database = open()
        val wire = database.create("Container", """{"title":"t","shelves":["PRIMARY","FUTURE"]}""", "{}")
        val container = Container.read(wire)
        assertEquals(listOf(ContainerShelf.Primary, ContainerShelf.Unknown("FUTURE")), container.shelves)
    }

    @Test
    fun theTypedApiWritesAListOfEnumValuesAndAnAbsentListReadsAsNull() = bounded {
        open()
        assertEquals(listOf(ContainerShelf.Secondary), Container.create(title = "t", shelves = listOf(ContainerShelf.Secondary)).shelves)
        assertNull(Container.create(title = "u").shelves)
    }

    @Test
    fun aListOfEnumValuesEncodesAsTheirTexts() {
        val serializer = ListSerializer(ContainerShelf.serializer())
        assertEquals("""["PRIMARY","x"]""", dbJson.encodeToString(serializer, listOf(ContainerShelf.Primary, ContainerShelf.Unknown("x"))))
    }

    @Test
    fun aScopeParamOfASchemaEnumIsSentAsItsText() {
        assertEquals("""{"model":"Inbox","relation":"shelf","params":{"list":"PRIMARY"}}""", Inbox.shelf(ContainerShelfKind.Primary).json)
        assertEquals("""{"model":"Inbox","relation":"shelf","params":{"list":"LATER"}}""", Inbox.shelf(ContainerShelfKind.Unknown("LATER")).json)
        assertNotEquals(Inbox.shelf(ContainerShelfKind.Secondary).json, Inbox.shelf(ContainerShelfKind.Primary).json)
    }
}
