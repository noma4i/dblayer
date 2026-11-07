package com.noma4i.dblayer

import okhttp3.OkHttpClient

class OkHttpEncryptedHttp(
    client: OkHttpClient = OkHttpClient(),
    private val headers: () -> Map<String, String> = { emptyMap() },
) : EncryptedHttp {
    private val client = client.newBuilder()
        .followRedirects(false)
        .followSslRedirects(false)
        .retryOnConnectionFailure(false)
        .build()

    override fun appHeaders(): Map<String, String> = headers()

    override suspend fun send(request: HttpRequest): RawOutcome = client.run(request)
}
