package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
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
            val encrypted = EncryptedConfig("https://api.example.com", SealedServer.SERVER_KEY, SealedServer.SERVER_PIN, EncryptedEndpoints("/api/graphql", "/api/cable", "/api/logout", "/api/bootstrap", mapOf("login_link" to EncryptedSessionOperation("/api/auth/login_link", false))), "example-ws-v1", "ws.hello", "EncryptedGraphqlChannel")
            DBlayer.openTesting(peer, modelsJSON, encrypted = encrypted, credentials = CableCredentials(), http = server)
            DBlayer.database.registerOperations("""[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]""")
            DBlayer.database.sessionCall("login_link", """{"account":"A"}""")
            val query = """{"model":"Notice","relation":"recent","params":{}}"""
            val sink = object : ValueSink { override fun delivered(delivery: StreamDelivery) {} }
            val first = DBlayer.database.subscribe(DBlayer.database.sessionEpoch(), StreamRequest.Query(query), sink)
            val old = peer.subscribed.receive()
            DBlayer.sessionReset()
            assertEquals(old, peer.closed.receive())
            DBlayer.database.sessionCall("login_link", """{"account":"B"}""")
            val second = DBlayer.database.subscribe(DBlayer.database.sessionEpoch(), StreamRequest.Query(query), sink)
            val current = peer.subscribed.receive()
            assertNotEquals(old, current)
            peer.emitOld(old)
            assertTrue(DBlayer.database.readQuery(DBlayer.database.sessionEpoch(), query).isEmpty())
            DBlayer.database.cancelStream(first)
            DBlayer.database.cancelStream(second)
        } finally {
            DBlayer.close()
        }
    }
}
