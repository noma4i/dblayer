package com.noma4i.dblayer

import okhttp3.OkHttpClient
import okhttp3.Request

/** The HTTP and WebSocket executor of the platform. */
class OkHttpTransport(
    private val headers: () -> Map<String, String> = { emptyMap() },
    client: OkHttpClient = OkHttpClient(),
) : Transport, EncryptedHttp {
    private val client = client.newBuilder().followRedirects(false).followSslRedirects(false).retryOnConnectionFailure(false).build()
    private val cable = OkHttpCable(this.client)

    override fun appHeaders(): Map<String, String> = headers()

    override suspend fun send(request: HttpRequest): RawOutcome = client.run(request)

    override suspend fun openCable(url: String, headers: Map<String, String>, connectMs: ULong, events: CableEvents): CableOutcome = cable.open(url, headers, connectMs, events)

    override suspend fun put(target: PutTarget, file: PutFile, identity: TransferIdentity, progress: ByteProgress): RawOutcome =
        putFile(client, target.url, target.headers, target.connectMs, target.requestMs, file, progress, target.responseLimit, routed = true)
}

internal suspend fun putFile(client: OkHttpClient, url: String, headers: Map<String, String>, connectMs: ULong, requestMs: ULong, file: PutFile, progress: ByteProgress, responseLimit: ULong, routed: Boolean = false): RawOutcome {
    val body = object : okhttp3.RequestBody() {
        override fun contentType() = null
        override fun contentLength() = file.length.toLong()
        override fun writeTo(sink: okio.BufferedSink) {
            java.io.RandomAccessFile(file.path, "r").use { input ->
                input.seek(file.offset.toLong())
                val buffer = ByteArray(64 * 1024)
                var sent = 0L
                while (sent < file.length.toLong()) {
                    val read = input.read(buffer, 0, minOf(buffer.size.toLong(), file.length.toLong() - sent).toInt())
                    if (read < 0) throw java.io.EOFException("the upload file is shorter than ${file.length} bytes")
                    sink.write(buffer, 0, read)
                    sent += read
                    progress.sent(sent.toULong())
                }
            }
        }
    }
    val request = try {
        Request.Builder().url(url).put(body).apply { headers.forEach { (field, value) -> header(field, value) } }.build()
    } catch (error: IllegalArgumentException) {
        return RawOutcome.Failed(urlFailure())
    }
    return try {
        (if (routed) UploadNetwork.route(client) else client).timed(connectMs, requestMs, total = false).newCall(request).exchange { RawOutcome.Response(it.code.toUShort(), it.header("ETag"), it.text(responseLimit)) }
    } catch (error: java.io.IOException) {
        RawOutcome.Failed(rawFailure(error))
    }
}
