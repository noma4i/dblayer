package com.noma4i.dblayer

import com.noma4i.dblayer.models.Notice
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private class CableCredentials : CredentialStore {
    @Volatile private var value: String? = null
    override fun get() = value
    override fun set(credential: String) { value = credential }
    override fun clear() { value = null }
}

@OptIn(DBlayerTestingApi::class)
class EncryptedOldSinkTest {
    @Test
    fun oldEncryptedCableCannotDeliverIntoTheNextAccount() = bounded {
        try {
            DBlayer.close()
            val server = SealedServer { path, body ->
                if (path == "/api/auth/login_link") {
                    Json.parseToJsonElement("""{"sessionId":${body.getValue("account")}}""").jsonObject
                } else Json.parseToJsonElement("""{"data":{}}""").jsonObject
            }
            val peer = EncryptedCablePeer(server)
            val encrypted = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", EncryptedOperationClass.AUTHENTICATED))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
            DBlayer.openTesting(peer, modelsJSON, encrypted = encrypted, credentials = CableCredentials(), http = server)
            DBlayer.database.registerOperations("""[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]""")
            DBlayer.database.setOnline(true)
            DBlayer.database.sessionCall("login_link", """{"account":"A"}""")
            val query = Notice.recent.json
            val first = AccountScope.current().stream(Notice.recent)
            val firstReading = launch(Dispatchers.Default) { runCatching { first.values.collect { } } }
            val old = peer.subscribed.receive()
            DBlayer.sessionReset()
            assertEquals(old, peer.closed.receive())
            DBlayer.database.sessionCall("login_link", """{"account":"B"}""")
            val second = AccountScope.current().stream(Notice.recent)
            val secondReading = launch(Dispatchers.Default) { runCatching { second.values.collect { } } }
            val current = peer.subscribed.receive()
            assertNotEquals(old, current)
            peer.emitOld(old)
            assertTrue(DBlayer.database.readQuery(DBlayer.database.sessionEpoch(), query).isEmpty())
            firstReading.cancel()
            secondReading.cancel()
            first.cancel()
            second.cancel()
        } finally {
            DBlayer.close()
        }
    }
}
