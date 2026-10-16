package com.noma4i.dblayer

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException

class OkHttpEncryptedHttp(
    private val origin: String,
    private val client: OkHttpClient = OkHttpClient(),
) : EncryptedHttp {
    override suspend fun post(path: String, body: String): EncryptedReply =
        send(Request.Builder().url(origin.trimEnd('/') + path).post(body.toByteArray().toRequestBody("application/jose".toMediaType())).build())

    override suspend fun get(path: String): EncryptedReply = send(Request.Builder().url(origin.trimEnd('/') + path).get().build())

    private suspend fun send(request: Request): EncryptedReply = try {
        client.newCall(request).exchange { EncryptedReply(it.code.toUShort(), it.body.string()) }
    } catch (error: IOException) {
        throw DbException.Network(error.message ?: error.toString())
    }
}
