package com.noma4i.dblayer

import java.io.IOException
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request

internal data class DriveChangePage(
    val nextPageToken: String?,
    val newStartPageToken: String?,
    val changes: List<DriveFileChange>,
)

internal data class DriveFileChange(
    val fileId: String,
    val removed: Boolean,
    val appProperties: Map<String, String>,
)

@Serializable
private data class StartToken(val startPageToken: String)

@Serializable
private data class ChangeFile(val appProperties: Map<String, String> = emptyMap())

@Serializable
private data class Change(val fileId: String, val removed: Boolean = false, val file: ChangeFile? = null)

@Serializable
private data class ChangePage(
    val nextPageToken: String? = null,
    val newStartPageToken: String? = null,
    val changes: List<Change> = emptyList(),
)

internal class DriveChanges(private val baseUrl: HttpUrl = "https://www.googleapis.com/drive/v3/".toHttpUrl()) {
    private val client = OkHttpClient.Builder().followRedirects(false).followSslRedirects(false).build()
    private val json = Json { ignoreUnknownKeys = true }

    suspend fun startPageToken(token: String): String {
        val url = baseUrl.newBuilder().addPathSegments("changes/startPageToken").build()
        return json.decodeFromString<StartToken>(get(token, url)).startPageToken.also { require(it.isNotEmpty()) }
    }

    suspend fun page(token: String, cursor: String): DriveChangePage {
        val url = baseUrl.newBuilder().addPathSegment("changes")
            .addQueryParameter("spaces", "appDataFolder")
            .addQueryParameter("pageToken", cursor)
            .addQueryParameter("fields", "nextPageToken,newStartPageToken,changes(fileId,removed,file(appProperties))")
            .build()
        val page = json.decodeFromString<ChangePage>(get(token, url))
        require((page.nextPageToken != null) xor (page.newStartPageToken != null))
        require(!(page.nextPageToken ?: page.newStartPageToken).isNullOrEmpty())
        return DriveChangePage(page.nextPageToken, page.newStartPageToken, page.changes.map {
            require(it.fileId.isNotEmpty())
            DriveFileChange(it.fileId, it.removed, it.file?.appProperties ?: emptyMap())
        })
    }

    private suspend fun get(token: String, url: HttpUrl): String {
        val request = Request.Builder().url(url).header("Authorization", "Bearer $token").get().build()
        return client.newCall(request).exchange { response ->
            if (!response.isSuccessful) throw IOException("HTTP ${response.code}")
            response.text(1_048_576uL)
        }
    }
}
