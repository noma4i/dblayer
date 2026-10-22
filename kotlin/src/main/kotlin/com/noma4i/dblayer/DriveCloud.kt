package com.noma4i.dblayer

internal sealed class DriveCursor {
    data class Files(val start: String, val page: String) : DriveCursor()
    data class Changes(val token: String) : DriveCursor()

    fun encode(): ByteArray = when (this) {
        is Files -> "f\n$start\n$page"
        is Changes -> "c\n$token"
    }.toByteArray(Charsets.UTF_8)

    companion object {
        fun parse(bytes: ByteArray?): DriveCursor? {
            val parts = bytes?.toString(Charsets.UTF_8)?.split('\n') ?: return null
            return when {
                parts.size == 3 && parts[0] == "f" -> Files(parts[1], parts[2])
                parts.size == 2 && parts[0] == "c" -> Changes(parts[1])
                else -> null
            }
        }
    }
}

class DriveCloud internal constructor(private val api: DriveApi, private val token: DriveToken) : CloudIo {
    override suspend fun account(): CloudAccount = try {
        token.accessToken()
        CloudAccount.AVAILABLE
    } catch (error: CloudException.NeedsAuthorization) {
        CloudAccount.NEEDS_CONSENT
    }

    override suspend fun pull(cursor: ByteArray?, limit: UInt): CloudPage {
        val size = limit.toInt().coerceIn(1, 100)
        return when (val at = DriveCursor.parse(cursor)) {
            null -> fromFiles(api.startPageToken(), null, size)
            is DriveCursor.Files -> fromFiles(at.start, at.page, size)
            is DriveCursor.Changes -> fromChanges(at.token, size)
        }
    }

    override suspend fun push(blobs: List<CloudBlob>) {
        for (blob in blobs) api.create(blob.key, blob.bytes)
    }

    private suspend fun fromFiles(start: String, page: String?, size: Int): CloudPage {
        val listed = api.listFiles(page, size)
        val next = listed.next
        val cursor = if (next != null) DriveCursor.Files(start, next) else DriveCursor.Changes(start)
        return CloudPage(read(listed.files), cursor.encode(), next != null)
    }

    private suspend fun fromChanges(token: String, size: Int): CloudPage {
        val listed = api.listChanges(token, size)
        val next = listed.next
        val cursor = DriveCursor.Changes(next ?: listed.newStart ?: token)
        return CloudPage(read(listed.files), cursor.encode(), next != null)
    }

    private suspend fun read(files: List<DriveFile>): List<CloudBlob> =
        files.map { CloudBlob(it.name ?: it.id, api.download(it.id)) }
}
