package com.noma4i.dblayer

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Authenticator
import okhttp3.CookieJar
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import java.util.concurrent.TimeUnit

class OkHttpEncryptedHttp(
    private val origin: String,
    client: OkHttpClient = OkHttpClient(),
) : EncryptedHttp {
    private val client = client.newBuilder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .callTimeout(30, TimeUnit.SECONDS)
        .followRedirects(false)
        .followSslRedirects(false)
        .retryOnConnectionFailure(false)
        .cookieJar(CookieJar.NO_COOKIES)
        .authenticator(Authenticator.NONE)
        .proxyAuthenticator(Authenticator.NONE)
        .cache(null)
        .apply { interceptors().clear(); networkInterceptors().clear() }
        .build()
    override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply =
        send(Request.Builder().url(origin.trimEnd('/') + path).post(body.toByteArray().toRequestBody("application/jose".toMediaType())).build().withUserAgent(), purpose)


    private suspend fun send(request: Request, purpose: NetworkPurpose): EncryptedReply = try {
        (if (purpose == NetworkPurpose.TRANSFER) UploadNetwork.route(client) else client).newCall(request).exchange { EncryptedReply(it.code.toUShort(), it.body.string()) }
    } catch (error: IOException) {
        throw classifyEncryptedHttpFailure(error)
    }
}
