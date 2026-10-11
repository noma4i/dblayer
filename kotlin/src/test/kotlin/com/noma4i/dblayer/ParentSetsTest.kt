package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.Message
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class ParentSetsTest {
    @Test
    fun aLocalMessageMovesTheChatPointerAndDiscardingItGivesThePointerBack() = runBlocking {
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, null)
        val chat = Chat.create(title = "t")
        val message = Message.create(chatId = chat.id, authorId = "u1", text = "hi")

        val pointed = withTimeout(5000) { Chat.find(chat.id).current() }
        assertEquals(message.id, pointed?.lastMessageId)
        assertNotNull(pointed?.lastMessageAt)

        message.discard()
        val restored = withTimeout(5000) { Chat.find(chat.id).current() }
        assertNull(restored?.lastMessageId)
        assertNull(restored?.lastMessageAt)
    }
}
