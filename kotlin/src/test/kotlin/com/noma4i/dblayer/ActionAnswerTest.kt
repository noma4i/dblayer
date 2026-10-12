package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class ActionAnswerTest {
    @Test
    fun anActionAnswersWithTheDeclaredFieldsOfThePayloadAndRefusesWhenTheServerRefuses() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        DBlayer.database = Database.open(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"},{"name":"ShareMoment","document":"mutation ShareMoment(${'$'}momentId: ID!) { shareMoment(momentId: ${'$'}momentId) { moment { id uuid likedByMe media createdAt } successMessage copies } }"}]""")
        val local = Moment.create(text = "hello", media = null)
        withTimeout(5000) { local.settled() }
        val moment = Moment.find(withTimeout(5000) { local.confirmedId() }).current()!!

        transport.enqueue("ShareMoment", """{"data":{"shareMoment":{"moment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"},"successMessage":"Shared","copies":3,"other":"x"}}}""")
        val shared = moment.share()
        assertEquals("m77", shared.model.id)
        val answer = withTimeout(5000) { shared.answer() }
        assertEquals("Shared", answer.successMessage)
        assertEquals(3, answer.copies)
        assertThrows(Exception::class.java) { bounded { shared.answer() } }

        transport.enqueue("ShareMoment", """{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}""")
        val refused = moment.share()
        eventually("not (transport.calls.count { it.operationName == \"ShareMoment\" } < 2)", 5000) { !(transport.calls.count { it.operationName == "ShareMoment" } < 2) }
        delay(100)
        assertThrows(Exception::class.java) { bounded { refused.answer() } }
        Unit
    }
}
