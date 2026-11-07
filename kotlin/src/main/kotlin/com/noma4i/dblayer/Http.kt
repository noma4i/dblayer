package com.noma4i.dblayer

import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response as HttpResponse
import java.io.IOException
import java.net.ConnectException
import java.net.NoRouteToHostException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLPeerUnverifiedException
import kotlin.coroutines.resumeWithException

internal fun Request.withUserAgent(): Request = newBuilder().header("User-Agent", userAgent(header("User-Agent"))).build()

internal fun rawFailure(error: Throwable): RawFailure {
    val kind = when (error) {
        is kotlinx.coroutines.CancellationException -> RawFailureKind.CANCELLED
        is SSLHandshakeException, is SSLPeerUnverifiedException -> RawFailureKind.TLS
        is UnknownHostException, is ConnectException, is NoRouteToHostException -> RawFailureKind.UNREACHABLE
        is SocketTimeoutException -> RawFailureKind.TIMEOUT
        else -> RawFailureKind.IO
    }
    return RawFailure(kind, null, error.message ?: error.toString())
}

internal fun failed(kind: RawFailureKind, message: String): RawOutcome = RawOutcome.Failed(RawFailure(kind, null, message))

internal suspend fun <T> Call.exchange(read: (HttpResponse) -> T): T = suspendCancellableCoroutine { continuation ->
    enqueue(object : Callback {
        override fun onFailure(call: Call, e: IOException) = continuation.resumeWithException(e)
        override fun onResponse(call: Call, response: HttpResponse) {
            val result = runCatching { response.use(read) }
            continuation.resumeWith(result)
        }
    })
    continuation.invokeOnCancellation { cancel() }
}

internal fun OkHttpClient.timed(connectMs: ULong, requestMs: ULong): OkHttpClient = newBuilder()
    .connectTimeout(connectMs.toLong(), TimeUnit.MILLISECONDS)
    .readTimeout(requestMs.toLong(), TimeUnit.MILLISECONDS)
    .writeTimeout(requestMs.toLong(), TimeUnit.MILLISECONDS)
    .callTimeout(requestMs.toLong(), TimeUnit.MILLISECONDS)
    .build()

internal suspend fun OkHttpClient.run(request: HttpRequest, appHeaders: Map<String, String>): RawOutcome {
    val http = try {
        Request.Builder().url(request.url).method(request.method, request.body.toRequestBody()).apply {
            appHeaders.forEach { (field, value) -> header(field, value) }
            request.headers.forEach { (field, value) -> header(field, value) }
        }.build().withUserAgent()
    } catch (error: IllegalArgumentException) {
        return failed(RawFailureKind.IO, error.message ?: "invalid request address")
    }
    return try {
        val routed = if (request.purpose == NetworkPurpose.TRANSFER) UploadNetwork.route(this) else this
        routed.timed(request.connectMs, request.requestMs).newCall(http).exchange { RawOutcome.Response(it.code.toUShort(), null, it.body.string()) }
    } catch (error: IOException) {
        RawOutcome.Failed(rawFailure(error))
    }
}
