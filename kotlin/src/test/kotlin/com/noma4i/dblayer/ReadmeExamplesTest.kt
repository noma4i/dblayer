package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.serialization.Serializable
import org.junit.Assert.assertEquals
import org.junit.Test

class ReadmeExamplesTest {
    @Serializable
    private class PhoneBody(val phone: String)

    @Test
    fun readme_examples_run_on_both_platforms() = bounded {
        closeForCleanup()
        val transport = MockTransport()
        transport.enqueue("CreateMoment", """{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","userId":"u1","text":"hello","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}""")
        openSigned(Config(null, null), modelsJSON, transport, SEND_MOMENT)
        val scope = AccountScope.current()
        val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val query = Moment.where(Moment.userId eq "u1").order(Moment.createdAt.desc).limit(20)
            val moments = Projected(scope.stream(query), query.decoder, screen)
            val moment = Moment.create(text = "hello", account = scope).optimisticValue
            assertEquals("hello", moment?.text)
            moments.until("the row of the sent moment reaches the feed") { it.rows.map { it.text } == listOf("hello") }

            val user = User.create(name = "ann").optimisticValue!!
            val source = java.io.File.createTempFile("attachment", ".png")
            source.deleteOnExit()
            source.writeBytes(byteArrayOf(1, 2, 3))
            val info = user.attach(User.avatar, source, name = "a.png", mime = "image/png")
            assertEquals(Triple("a.png", "image/png", 3UL), Triple(info.name, info.mime, info.size))

            DBlayer.sessionCall<PhoneBody, kotlinx.serialization.json.JsonElement>("login_link", PhoneBody("+61400000000"))
            Unit
        } finally {
            screen.cancel()
        }
    }
}
