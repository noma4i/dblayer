package com.noma4i.dblayer

import okhttp3.OkHttpClient
import okhttp3.Request

/** A plain GraphQL endpoint over HTTP. */
class OkHttpTransport(
    private val endpoint: String,
    private val headers: () -> Map<String, String> = { emptyMap() },
    client: OkHttpClient = OkHttpClient(),
) : Transport {
    private val client = client.newBuilder().followRedirects(false).followSslRedirects(false).retryOnConnectionFailure(false).build()
    private val cable = OkHttpCable(this.client, headers)

    override fun endpoint(): String = endpoint

    override suspend fun send(request: HttpRequest): RawOutcome = client.run(request, headers())

    override suspend fun openCable(url: String, connectMs: ULong, events: CableEvents): CableOutcome = cable.open(url, connectMs, events)

    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): RawOutcome =
        putFile(client, url, headers, file, progress, routed = true)
}

internal suspend fun putFile(client: OkHttpClient, url: String, headers: Map<String, String>, file: PutFile, progress: ByteProgress, routed: Boolean = false): RawOutcome {
    val body = object : okhttp3.RequestBody() {
        override fun contentType() = null
        override fun contentLength() = file.length.toLong()
        override fun writeTo(sink: okio.BufferedSink) {
            java.io.RandomAccessFile(file.path, "r").use { input ->
                input.seek(file.offset.toLong())
                val buffer = ByteArray(64 * 1024)
                var sent = 0L
                while (sent < file.length.toLong()) {
                    if (progress.isCancelled()) throw java.io.IOException("cancelled")
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
        Request.Builder().url(url).put(body).apply { headers.forEach { (field, value) -> header(field, value) } }.build().withUserAgent()
    } catch (error: IllegalArgumentException) {
        return failed(RawFailureKind.IO, error.message ?: "invalid upload address")
    }
    return try {
        (if (routed) UploadNetwork.route(client) else client).newCall(request).exchange { RawOutcome.Response(it.code.toUShort(), it.header("ETag"), "") }
    } catch (error: java.io.IOException) {
        RawOutcome.Failed(rawFailure(error))
    }
}
