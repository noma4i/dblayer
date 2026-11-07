package com.noma4i.dblayer

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException

class OkHttpEncryptedHttp(
    private val origin: String,
    client: OkHttpClient = OkHttpClient(),
    private val headers: () -> Map<String, String> = { emptyMap() },
) : EncryptedHttp {
    private val client = client.newBuilder()
        .followRedirects(false)
        .followSslRedirects(false)
        .retryOnConnectionFailure(false)
        .build()
    override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply =
        send(Request.Builder().url(origin.trimEnd('/') + path).post(body.toByteArray().toRequestBody("application/jose".toMediaType())).apply { headers().forEach { (field, value) -> header(field, value) } }.build().withUserAgent(), purpose)


    private suspend fun send(request: Request, purpose: NetworkPurpose): EncryptedReply = try {
        (if (purpose == NetworkPurpose.TRANSFER) UploadNetwork.route(client) else client).newCall(request).exchange { EncryptedReply(it.code.toUShort(), it.body.string()) }
    } catch (error: IOException) {
        throw classifyEncryptedHttpFailure(error)
    }
}
