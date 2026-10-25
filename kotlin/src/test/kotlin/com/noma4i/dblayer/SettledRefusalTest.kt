package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class SettledRefusalTest {
    @Test
    fun aRefusalAnsweredBeforeSettledStartedIsStillThrownOnce() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"},{"name":"LikeMoment","document":"mutation LikeMoment(${'$'}momentId: ID!) { likeMoment(momentId: ${'$'}momentId) { id uuid likedByMe media createdAt } }"}]""")
        val local = Moment.create(text = "hello", media = null)
        withTimeout(5000) { local.settled(null) }
        val moment = Moment.find(withTimeout(5000) { local.confirmedId() }).current()!!

        transport.enqueue("LikeMoment", """{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}""")
        val liked = moment.like()
        assertEquals(true, liked.likedByMe)
        eventually("not (Moment.find(moment.id).current()?.likedByMe != false)", 5000) { !(Moment.find(moment.id).current()?.likedByMe != false) }

        assertThrows(Exception::class.java) { bounded { liked.settled(null) } }
        liked.settled(null)
    }
}
