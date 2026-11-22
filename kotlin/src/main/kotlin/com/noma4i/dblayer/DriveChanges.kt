package com.noma4i.dblayer

import java.io.IOException
import java.io.File
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okio.buffer
import okio.sink

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

internal data class DriveFilePage(val nextPageToken: String?, val files: List<DriveFile>)

internal data class DriveFile(val id: String, val replica: String)

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

@Serializable
private data class ListedFile(val id: String, val appProperties: Map<String, String> = emptyMap())

@Serializable
private data class ListedPage(val nextPageToken: String? = null, val files: List<ListedFile> = emptyList())

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

    suspend fun files(token: String, domain: String, cursor: String?): DriveFilePage {
        require(domain.length == 64 && domain.all { it in '0'..'9' || it in 'a'..'f' })
        val url = baseUrl.newBuilder().addPathSegment("files")
            .addQueryParameter("spaces", "appDataFolder")
            .addQueryParameter("fields", "nextPageToken,files(id,appProperties)")
            .addQueryParameter("q", "appProperties has { key='domain' and value='$domain' }")
            .apply { if (cursor != null) addQueryParameter("pageToken", cursor) }
            .build()
        val page = json.decodeFromString<ListedPage>(get(token, url))
        require(page.nextPageToken == null || page.nextPageToken.isNotEmpty())
        return DriveFilePage(page.nextPageToken, page.files.filter { it.appProperties["domain"] == domain }.map {
            require(it.id.isNotEmpty())
            DriveFile(it.id, requireNotNull(it.appProperties["replica"]).also { replica -> require(replica.isNotEmpty()) })
        })
    }

    suspend fun download(token: String, fileId: String, destination: File) {
        val url = baseUrl.newBuilder().addPathSegment("files").addPathSegment(fileId)
            .addQueryParameter("alt", "media").build()
        try {
            request(token, url) { response ->
                try {
                    destination.sink().buffer().use { it.writeAll(response.body.source()) }
                } catch (error: Throwable) {
                    destination.delete()
                    throw error
                }
            }
        } catch (error: Throwable) {
            destination.delete()
            throw error
        }
    }

    private suspend fun get(token: String, url: HttpUrl): String = request(token, url) { it.text(1_048_576uL) }

    private suspend fun <T> request(token: String, url: HttpUrl, read: (okhttp3.Response) -> T): T {
        val request = Request.Builder().url(url).header("Authorization", "Bearer $token").get().build()
        return client.newCall(request).exchange { response ->
            if (!response.isSuccessful) throw IOException("HTTP ${response.code}")
            read(response)
        }
    }
}
