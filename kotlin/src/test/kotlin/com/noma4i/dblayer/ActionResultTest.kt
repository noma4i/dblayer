package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.MomentShareResult
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class ActionResultTest {
    @Test
    fun anActionAnswersWithTheDeclaredFieldsOfThePayloadAndRefusesWhenTheServerRefuses() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"},{"name":"ShareMoment","document":"mutation ShareMoment(${'$'}momentId: ID!) { shareMoment(momentId: ${'$'}momentId) { moment { id uuid likedByMe media createdAt } successMessage copies } }"}]""")
        val moment = Moment.find(withTimeout(5000) { Moment.create(text = "hello", media = null, account = AccountScope.current()).confirmed() }.id).current()!!

        transport.enqueue("ShareMoment", """{"data":{"shareMoment":{"moment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"},"successMessage":"Shared","copies":3,"other":"x"}}}""")
        val shared = moment.share()
        assertEquals("m77", shared.optimisticValue?.id)
        val answer = withTimeout(5000) { shared.answer<MomentShareResult>() }
        assertEquals("Shared", answer.successMessage)
        assertEquals(3L, answer.copies)
        assertEquals(3L, withTimeout(5000) { shared.answer<MomentShareResult>() }.copies)

        transport.enqueue("ShareMoment", """{"errors":[{"message":"not allowed","extensions":{"code":"FORBIDDEN"}}]}""")
        val refused = moment.share()
        eventually("not (transport.calls.count { it.operationName == \"ShareMoment\" } < 2)", 5000) { !(transport.calls.count { it.operationName == "ShareMoment" } < 2) }
        assertEquals("not allowed", withTimeout(5000) { refused.refusal() }.message)
        Unit
    }

    @Test
    fun anActionAnswersWhenEveryFieldOfThePayloadIsNull() = bounded {
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        reopen(Config(null, null, 32u), modelsJSON, transport)
        DBlayer.database.registerOperations("""[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"},{"name":"ShareMoment","document":"mutation ShareMoment(${'$'}momentId: ID!) { shareMoment(momentId: ${'$'}momentId) { moment { id uuid likedByMe media createdAt } successMessage copies } }"}]""")
        val moment = Moment.find(withTimeout(5000) { Moment.create(text = "hello", media = null, account = AccountScope.current()).confirmed() }.id).current()!!

        transport.enqueue("ShareMoment", """{"data":{"shareMoment":{"moment":null,"successMessage":null,"copies":null}}}""")
        val answer = withTimeout(5000) { moment.share().answer<MomentShareResult>() }
        assertNull(answer.successMessage)
        assertNull(answer.copies)
        assertNotNull(Moment.find("m77").current())
        Unit
    }
}
