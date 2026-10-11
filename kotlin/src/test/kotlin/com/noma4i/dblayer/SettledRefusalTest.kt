package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class SettledRefusalTest {
    @Test
    fun aRefusalAnsweredBeforeSettledStartedIsStillThrownOnce() = runBlocking {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"},{"name":"LikeMoment","document":"mutation LikeMoment(${'$'}momentId: ID!) { likeMoment(momentId: ${'$'}momentId) { id uuid likedByMe media createdAt } }"}]""")
        val local = Moment.create(text = "hello", media = null)
        withTimeout(5000) { local.settled() }
        val moment = Moment.find(withTimeout(5000) { local.confirmedId() }).current()!!

        transport.enqueue("LikeMoment", """{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}""")
        val liked = moment.like()
        assertEquals(true, liked.likedByMe)
        withTimeout(5000) { while (Moment.find(moment.id).current()?.likedByMe != false) delay(10) }

        assertThrows(Exception::class.java) { runBlocking { liked.settled() } }
        liked.settled()
    }
}
