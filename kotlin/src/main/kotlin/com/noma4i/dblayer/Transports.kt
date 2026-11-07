package com.noma4i.dblayer

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody

/** A plain GraphQL endpoint over HTTP. */
class OkHttpTransport(
    private val endpoint: String,
    private val headers: () -> Map<String, String> = { emptyMap() },
    client: OkHttpClient = OkHttpClient(),
) : Transport {
    private val client = client.newBuilder().followRedirects(false).followSslRedirects(false).retryOnConnectionFailure(false).build()
    private val encryptedSocket = OkHttpEncryptedSocket(client, headers)
    override suspend fun openCable(lease: CableLease, url: String) = lease.opened(encryptedSocket.open(url))
    override suspend fun cableSend(token: String, text: String) = encryptedSocket.send(token, text)
    override suspend fun cableNext(token: String): EncryptedSocketEvent? = encryptedSocket.next(token)
    override suspend fun cableClose(token: String) = encryptedSocket.close(token)

    private fun body(request: TransportRequest): String = kotlinx.serialization.json.buildJsonObject {
        put("query", kotlinx.serialization.json.JsonPrimitive(request.document))
        put("operationName", kotlinx.serialization.json.JsonPrimitive(request.operationName))
        put("variables", parseJson(request.variablesJson))
    }.toString()

    private suspend fun post(request: TransportRequest, kind: GraphQLOperationKind): String {
        val http = Request.Builder().url(endpoint).post(body(request).toRequestBody("application/json".toMediaType())).apply {
            headers().forEach { (field, value) -> header(field, value) }
        }.build().withUserAgent()
        return try {
            (if (request.purpose == NetworkPurpose.TRANSFER) UploadNetwork.route(client) else client).newCall(http).exchange { response ->
                val text = response.body.string()
                if (response.code !in 200..299) throw DbException.Http(response.code.toUShort(), text)
                text
            }
        } catch (error: java.io.IOException) {
            throw classifyTransportFailure(error, kind)
        }
    }

    override suspend fun query(request: TransportRequest): String = post(request, GraphQLOperationKind.Query)

    override suspend fun mutate(request: TransportRequest): String = post(request, GraphQLOperationKind.Mutation)

    override suspend fun put(url: String, headers: Map<String, String>, file: PutFile, identity: TransferIdentity, progress: ByteProgress): PutReply =
        putFile(client, url, headers, file, progress, routed = true)
}

internal suspend fun putFile(client: OkHttpClient, url: String, headers: Map<String, String>, file: PutFile, progress: ByteProgress, routed: Boolean = false): PutReply {
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
    val request = Request.Builder().url(url).put(body).apply { headers.forEach { (field, value) -> header(field, value) } }.build().withUserAgent()
    return try {
        (if (routed) UploadNetwork.route(client) else client).newCall(request).exchange { PutReply(it.code.toUShort(), it.header("ETag")) }
    } catch (error: java.io.IOException) {
        if (progress.isCancelled()) throw DbException.Interrupted(error.message ?: "cancelled")
        throw DbException.Network(error.message ?: error.toString())
    }
}
