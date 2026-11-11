package com.noma4i.dblayer

import com.noma4i.dblayer.models.CurrentUser
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(DBlayerTestingApi::class)
class CableClockTest {
    @Test
    fun encryptedCableKeepsAnObservedProfileAcrossClockAdvances() = bounded {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            closeForCleanup()
            val schema = Json.parseToJsonElement(modelsJSON).jsonObject
            val models = schema.getValue("models").jsonArray.map { element ->
                val model = JsonObject(element.jsonObject - "change_feed")
                if (model["key"]?.jsonPrimitive?.content != "CurrentUser") model else JsonObject(model + mapOf(
                    "changes" to Json.parseToJsonElement("""[{"area":"COUNTERS","relation":"current"}]"""),
                    "change_feed" to Json.parseToJsonElement("""{"subscription":"DblayerViewerChanges","root":"viewerChanges.changes","area":"area","kind":"kind","id":"entityId"}"""),
                ))
            }
            val spec = JsonObject(schema + ("models" to JsonArray(models))).toString()
            val definitions = """[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"DblayerViewerChanges","document":"subscription DblayerViewerChanges { viewerChanges { changes { area kind entityId } } }"}]"""
            val transport = MockTransport()
            val clock = openSignedClocked(spec, transport, definitions)
            transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"Ann"}}}""")
            transport.enqueue("CurrentUserQuery", """{"data":{"user":{"id":"u1","name":"Bea"}}}""")
            val profile = Projected(AccountScope.current().stream(CurrentUser.current), CurrentUser, scope)
            CurrentUser.current.refresh()
            awaitWithin("the profile is shown") { profile.projection.first { profile.value?.name == "Ann" } }
            val peer = transport.cable as EncryptedCablePeer
            val token = awaitWithin("the profile subscription") { peer.subscribed.receive() }
            repeat(5) { step ->
                clock.advance(120_000L)
                assertEquals("the connection reopened", token, awaitWithin("ping on the original connection, which may have reopened, step=${step + 1}", detail = { "opens=${peer.openedCount} subscriptions=${peer.subscribedCount.get()} last=${peer.lastEvent}" }) { peer.ping.receive() })
            }
            val event = Json.parseToJsonElement("""{"data":{"viewerChanges":{"changes":[{"area":"COUNTERS","kind":"UPDATED","entityId":"u1"}]}}}""").jsonObject
            assertTrue(peer.event(token, event))
            awaitWithin("the change rereads the profile") { profile.projection.first { profile.value?.name == "Bea" } }
            assertEquals(1, peer.openedCount)
            assertEquals(2, transport.calls.count { it.operationName == "CurrentUserQuery" })
            profile.close()
        } finally {
            scope.cancel()
            closeForCleanup()
        }
    }
}
