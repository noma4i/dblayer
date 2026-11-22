package com.noma4i.dblayer

import java.io.IOException
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import okhttp3.Call
import okhttp3.Callback
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response

internal enum class DriveWatchEvent { READY, CHANGED }

internal class DriveWatch(private val relay: HttpUrl) {
    private companion object {
        val random = SecureRandom()
        val client = OkHttpClient.Builder().followRedirects(false).followSslRedirects(false)
            .readTimeout(0, TimeUnit.MILLISECONDS).build()
    }

    val secret: String = ByteArray(32).also(random::nextBytes)
        .let { Base64.getUrlEncoder().withoutPadding().encodeToString(it) }

    fun events(): Flow<DriveWatchEvent> = callbackFlow {
        val request = Request.Builder().url(relay.newBuilder().addPathSegment("events").build())
            .header("Authorization", "Bearer $secret").build()
        val call = client.newCall(request)
        call.enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) { close(e) }

            override fun onResponse(call: Call, response: Response) {
                try {
                    response.use {
                        if (!it.isSuccessful) throw DriveHttpException(it.code)
                        val source = it.body.source()
                        var event: String? = null
                        var data = false
                        while (!source.exhausted()) {
                            val line = source.readUtf8LineStrict(8192)
                            if (line.isEmpty()) {
                                val value = when (event) {
                                    "ready" -> DriveWatchEvent.READY
                                    "changed" -> DriveWatchEvent.CHANGED
                                    else -> null
                                }
                                if (value != null && data && trySend(value).isFailure) throw IOException("relay event delivery failed")
                                event = null
                                data = false
                            } else if (line.startsWith("event:")) {
                                event = line.substringAfter(':').trimStart(' ')
                            } else if (line.startsWith("data:")) {
                                data = line.substringAfter(':').trimStart(' ') == "{}"
                            }
                        }
                    }
                    close()
                } catch (error: Exception) {
                    close(error)
                }
            }
        })
        awaitClose { call.cancel() }
    }
}
