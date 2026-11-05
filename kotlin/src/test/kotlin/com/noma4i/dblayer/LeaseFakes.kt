package com.noma4i.dblayer

import okhttp3.mockwebserver.MockWebServer

class RecordingLease : CableLease(NoHandle) {
    @Volatile var token: String? = null
    override fun opened(token: String) { this.token = token }
}

fun cableUrl(server: MockWebServer): String = server.url("/api/cable").toString().replaceFirst("http", "ws")

suspend fun Transport.openedCable(url: String = "ws://localhost/api/cable"): String {
    val lease = RecordingLease()
    openCable(lease, url)
    return requireNotNull(lease.token) { "the transport finished openCable without opened" }
}
