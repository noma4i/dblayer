package com.noma4i.dblayer

import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.RecordedRequest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

internal class FakeDrive : Dispatcher() {
    private class Stored(val id: String, val name: String, val bytes: ByteArray, val seq: Int)

    private val files = mutableListOf<Stored>()
    val names: List<String> @Synchronized get() = files.map { it.name }
    var failNext: MockResponse? = null

    @Synchronized
    override fun dispatch(request: RecordedRequest): MockResponse {
        failNext?.let { failNext = null; return it }
        val url = request.requestUrl!!
        val path = url.encodedPath
        return when {
            request.method == "POST" -> create(request)
            path.endsWith("/changes/startPageToken") -> json("""{"startPageToken":"${files.size}"}""")
            path.endsWith("/changes") -> changes(url.queryParameter("pageToken")!!.toInt(), url.queryParameter("pageSize")!!.toInt())
            path.endsWith("/files") -> list(url.queryParameter("pageToken")?.toInt() ?: 0, url.queryParameter("pageSize")!!.toInt())
            else -> MockResponse().setBody(okio.Buffer().write(files.first { it.id == path.substringAfterLast('/') }.bytes))
        }
    }

    private fun create(request: RecordedRequest): MockResponse {
        val boundary = request.getHeader("Content-Type")!!.substringAfter("boundary=")
        val parts = request.body.readString(Charsets.ISO_8859_1).split("--$boundary").filter { it.contains("\r\n\r\n") }
        val metadata = Json.parseToJsonElement(parts[0].substringAfter("\r\n\r\n").trim()).jsonObject
        val content = parts[1].substringAfter("\r\n\r\n").removeSuffix("\r\n")
        val name = metadata.getValue("name").jsonPrimitive.content
        files += Stored("id${files.size}", name, content.toByteArray(Charsets.ISO_8859_1), files.size + 1)
        return json("""{"id":"id${files.size - 1}"}""")
    }

    private fun list(from: Int, size: Int): MockResponse {
        val page = files.drop(from).take(size)
        val next = if (from + size < files.size) ""","nextPageToken":"${from + size}"""" else ""
        return json("""{"files":[${page.joinToString(",") { """{"id":"${it.id}","name":"${it.name}"}""" }}]$next}""")
    }

    private fun changes(after: Int, size: Int): MockResponse {
        val page = files.filter { it.seq > after }
        val shown = page.take(size)
        val tail = if (page.size > size) """"nextPageToken":"${shown.last().seq}"""" else """"newStartPageToken":"${files.size}""""
        return json("""{"changes":[${shown.joinToString(",") { """{"removed":false,"file":{"id":"${it.id}","name":"${it.name}"}}""" }}],$tail}""")
    }

    private fun json(body: String) = MockResponse().setHeader("Content-Type", "application/json").setBody(body)
}
