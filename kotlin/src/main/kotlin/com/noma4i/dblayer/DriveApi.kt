package com.noma4i.dblayer

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.TimeUnit

internal data class DriveFile(val id: String, val name: String?)

internal data class DriveFilesPage(val files: List<DriveFile>, val next: String?)

internal data class DriveChangesPage(val files: List<DriveFile>, val next: String?, val newStart: String?)

internal fun interface DriveToken {
    suspend fun accessToken(): String
}

internal class DriveApi(
    client: OkHttpClient,
    private val token: DriveToken,
    private val root: HttpUrl = "https://www.googleapis.com/".toHttpUrl(),
) {
    private val client = client.newBuilder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .callTimeout(60, TimeUnit.SECONDS)
        .followRedirects(false)
        .build()

    suspend fun startPageToken(): String =
        json(get(root.newBuilder().addPathSegments("drive/v3/changes/startPageToken").build()))["startPageToken"]!!.jsonPrimitive.content

    suspend fun listFiles(pageToken: String?, pageSize: Int): DriveFilesPage {
        val url = root.newBuilder().addPathSegments("drive/v3/files")
            .addQueryParameter("spaces", "appDataFolder")
            .addQueryParameter("pageSize", pageSize.toString())
            .addQueryParameter("fields", "nextPageToken,files(id,name)")
            .apply { if (pageToken != null) addQueryParameter("pageToken", pageToken) }
            .build()
        val body = json(get(url))
        return DriveFilesPage(files(body["files"]), body["nextPageToken"]?.jsonPrimitive?.contentOrNull)
    }

    suspend fun listChanges(pageToken: String, pageSize: Int): DriveChangesPage {
        val url = root.newBuilder().addPathSegments("drive/v3/changes")
            .addQueryParameter("spaces", "appDataFolder")
            .addQueryParameter("pageToken", pageToken)
            .addQueryParameter("pageSize", pageSize.toString())
            .addQueryParameter("includeRemoved", "false")
            .addQueryParameter("fields", "nextPageToken,newStartPageToken,changes(removed,file(id,name,trashed))")
            .build()
        val body = json(get(url))
        val files = body["changes"]?.jsonArray.orEmpty().mapNotNull { change ->
            val entry = change.jsonObject
            val file = entry["file"]?.jsonObject
            if (entry["removed"]?.jsonPrimitive?.booleanOrNull == true || file == null || file["trashed"]?.jsonPrimitive?.booleanOrNull == true) null
            else DriveFile(file.getValue("id").jsonPrimitive.content, file["name"]?.jsonPrimitive?.contentOrNull)
        }
        return DriveChangesPage(files, body["nextPageToken"]?.jsonPrimitive?.contentOrNull, body["newStartPageToken"]?.jsonPrimitive?.contentOrNull)
    }

    suspend fun download(id: String): ByteArray {
        val url = root.newBuilder().addPathSegments("drive/v3/files").addPathSegment(id).addQueryParameter("alt", "media").build()
        return get(url)
    }

    suspend fun create(name: String, bytes: ByteArray) {
        val metadata = buildJsonObject {
            put("name", name)
            put("parents", buildJsonArray { add(kotlinx.serialization.json.JsonPrimitive("appDataFolder")) })
        }
        val body = MultipartBody.Builder().setType("multipart/related".toMediaType())
            .addPart(metadata.toString().toRequestBody("application/json; charset=UTF-8".toMediaType()))
            .addPart(bytes.toRequestBody("application/octet-stream".toMediaType()))
            .build()
        val url = root.newBuilder().addPathSegments("upload/drive/v3/files").addQueryParameter("uploadType", "multipart").addQueryParameter("fields", "id").build()
        send(Request.Builder().url(url).post(body), acceptConflict = true)
    }

    private suspend fun get(url: HttpUrl): ByteArray = send(Request.Builder().url(url).get(), acceptConflict = false)

    private suspend fun send(builder: Request.Builder, acceptConflict: Boolean): ByteArray {
        val request = builder.header("Authorization", "Bearer " + token.accessToken()).build().withUserAgent()
        return try {
            client.newCall(request).exchange { reply -> read(reply, acceptConflict) }
        } catch (error: IOException) {
            throw CloudException.Offline(error.message ?: error.javaClass.simpleName)
        }
    }

    private fun read(reply: Response, acceptConflict: Boolean): ByteArray {
        val bytes = reply.body.bytes()
        if (reply.isSuccessful || (acceptConflict && reply.code == 409)) return bytes
        throw driveError(reply.code, reply.header("Retry-After"), bytes.toString(Charsets.UTF_8))
    }

    private fun json(bytes: ByteArray): JsonObject = try {
        Json.parseToJsonElement(bytes.toString(Charsets.UTF_8)).jsonObject
    } catch (error: Exception) {
        throw CloudException.Rejected("unreadable Drive reply: ${error.message}")
    }

    private fun files(element: kotlinx.serialization.json.JsonElement?): List<DriveFile> =
        element?.jsonArray.orEmpty().map { DriveFile(it.jsonObject.getValue("id").jsonPrimitive.content, it.jsonObject["name"]?.jsonPrimitive?.contentOrNull) }
}
