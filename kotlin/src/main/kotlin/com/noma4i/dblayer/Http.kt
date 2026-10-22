package com.noma4i.dblayer

import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Request
import okhttp3.Response as HttpResponse
import java.io.IOException
import kotlin.coroutines.resumeWithException

internal enum class GraphQLOperationKind { Query, Mutation }

internal fun Request.withUserAgent(): Request = newBuilder().header("User-Agent", userAgent(header("User-Agent"))).build()

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
