package com.noma4i.dblayer

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Authenticator
import okhttp3.CookieJar
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException

class OkHttpEncryptedHttp(
    private val origin: String,
    client: OkHttpClient = OkHttpClient(),
) : EncryptedHttp {
    private val client = client.newBuilder()
        .followRedirects(false)
        .followSslRedirects(false)
        .retryOnConnectionFailure(false)
        .cookieJar(CookieJar.NO_COOKIES)
        .authenticator(Authenticator.NONE)
        .proxyAuthenticator(Authenticator.NONE)
        .cache(null)
        .apply { interceptors().clear(); networkInterceptors().clear() }
        .build()
    override suspend fun post(path: String, body: String): EncryptedReply =
        send(Request.Builder().url(origin.trimEnd('/') + path).post(body.toByteArray().toRequestBody("application/jose".toMediaType())).build().withUserAgent())


    private suspend fun send(request: Request): EncryptedReply = try {
        client.newCall(request).exchange { EncryptedReply(it.code.toUShort(), it.body.string()) }
    } catch (error: IOException) {
        throw classifyEncryptedHttpFailure(error)
    }
}
