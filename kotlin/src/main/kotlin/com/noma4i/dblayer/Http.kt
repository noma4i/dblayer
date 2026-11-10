package com.noma4i.dblayer

import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response as HttpResponse
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resumeWithException

internal class ResponseLimitException(limit: ULong) : IOException("the answer is above the limit of $limit bytes")

internal fun rawFailure(error: Throwable): RawFailure =
    if (error is ResponseLimitException) RawFailure("DBlayer.ResponseLimit", 1L, error.message ?: "") else RawFailure(error.javaClass.name, null, error.message ?: error.toString())

/** Reads the answer in pieces and stops at the first byte above `limit`: the body above the limit never sits in memory. */
internal fun HttpResponse.text(limit: ULong): String {
    val source = body.source()
    val buffer = okio.Buffer()
    val bound = limit.toLong()
    if (body.contentLength() > bound) throw ResponseLimitException(limit)
    while (source.read(buffer, 8192L) >= 0L) {
        if (buffer.size > bound) throw ResponseLimitException(limit)
    }
    return buffer.readUtf8()
}

internal fun urlFailure(): RawFailure = RawFailure("url", null, "")

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

internal fun OkHttpClient.timed(connectMs: ULong, requestMs: ULong, total: Boolean = true): OkHttpClient = newBuilder()
    .connectTimeout(connectMs.toLong(), TimeUnit.MILLISECONDS)
    .readTimeout(requestMs.toLong(), TimeUnit.MILLISECONDS)
    .writeTimeout(requestMs.toLong(), TimeUnit.MILLISECONDS)
    .callTimeout(if (total) requestMs.toLong() else 0L, TimeUnit.MILLISECONDS)
    .build()

internal suspend fun OkHttpClient.run(request: HttpRequest): RawOutcome {
    val http = try {
        Request.Builder().url(request.url).method(request.method, request.body.toRequestBody()).apply {
            request.headers.forEach { (field, value) -> header(field, value) }
        }.build()
    } catch (error: IllegalArgumentException) {
        return RawOutcome.Failed(urlFailure())
    }
    return try {
        val routed = if (request.purpose == NetworkPurpose.TRANSFER) UploadNetwork.route(this) else this
        routed.timed(request.connectMs, request.requestMs).newCall(http).exchange { RawOutcome.Response(it.code.toUShort(), null, it.text(request.responseLimit)) }
    } catch (error: IOException) {
        RawOutcome.Failed(rawFailure(error))
    }
}
