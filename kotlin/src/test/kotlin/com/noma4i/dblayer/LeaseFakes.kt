package com.noma4i.dblayer

import kotlinx.coroutines.channels.Channel
import okhttp3.mockwebserver.MockWebServer

class RecordingEvents : CableEvents {
    sealed interface Event {
        data class Frame(val text: String) : Event
        class Binary(val bytes: ByteArray) : Event
        data class Closed(val code: UShort, val reason: String) : Event
        data class Failed(val kind: RawFailureKind) : Event
    }

    val events = Channel<Event>(Channel.UNLIMITED)

    override fun frame(text: String) { events.trySend(Event.Frame(text)) }
    override fun binary(bytes: ByteArray) { events.trySend(Event.Binary(bytes)) }
    override fun closed(code: UShort, reason: String) { events.trySend(Event.Closed(code, reason)) }
    override fun failed(failure: RawFailure) { events.trySend(Event.Failed(failure.kind)) }

    suspend fun next(): Event = events.receive()
}

fun cableUrl(server: MockWebServer): String = server.url("/api/cable").toString().replaceFirst("http", "ws")

fun probeRequest(url: String, body: String = "jwe"): HttpRequest =
    HttpRequest(url, "POST", mapOf("Content-Type" to "application/jose", "Accept" to "", "Accept-Language" to ""), body, 10_000uL, 30_000uL, NetworkPurpose.STANDARD)

fun RawOutcome.status(): UShort? = (this as? RawOutcome.Response)?.status

fun RawOutcome.failureKind(): RawFailureKind? = (this as? RawOutcome.Failed)?.failure?.kind
