package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.ChatShelf
import com.noma4i.dblayer.models.ChatShelfKind
import com.noma4i.dblayer.models.Inbox
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.builtins.ListSerializer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Test

class EnumListTest {
    private fun open() = Database.open(Config(null, null, 32u), modelsJSON, null).also { DBlayer.database = it }

    @Test
    fun aListOfEnumValuesIsStoredReadBackAndKeepsAValueTheDeclarationLacks() = bounded {
        val database = open()
        val wire = database.create("Chat", """{"title":"t","shelves":["PRIMARY","FUTURE"]}""", "{}")
        val chat = Chat.read(wire)
        assertEquals(listOf(ChatShelf.Primary, ChatShelf.Unknown("FUTURE")), chat.shelves)
    }

    @Test
    fun theTypedApiWritesAListOfEnumValuesAndAnAbsentListReadsAsNull() = bounded {
        open()
        assertEquals(listOf(ChatShelf.Secondary), Chat.create(title = "t", shelves = listOf(ChatShelf.Secondary)).shelves)
        assertNull(Chat.create(title = "u").shelves)
    }

    @Test
    fun aListOfEnumValuesEncodesAsTheirTexts() {
        val serializer = ListSerializer(ChatShelf.serializer())
        assertEquals("""["PRIMARY","x"]""", dbJson.encodeToString(serializer, listOf(ChatShelf.Primary, ChatShelf.Unknown("x"))))
    }

    @Test
    fun aScopeParamOfASchemaEnumIsSentAsItsText() {
        assertEquals("""{"model":"Inbox","relation":"shelf","params":{"list":"PRIMARY"}}""", Inbox.shelf(ChatShelfKind.Primary).json)
        assertEquals("""{"model":"Inbox","relation":"shelf","params":{"list":"LATER"}}""", Inbox.shelf(ChatShelfKind.Unknown("LATER")).json)
        assertNotEquals(Inbox.shelf(ChatShelfKind.Secondary).json, Inbox.shelf(ChatShelfKind.Primary).json)
    }
}
