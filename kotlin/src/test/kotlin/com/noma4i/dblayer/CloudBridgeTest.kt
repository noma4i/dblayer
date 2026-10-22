package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private const val CLOUD_MODELS = """[{"key":"Note","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"},"optional":true}],"storage":"local","cloud":["title"]}]"""

private class RecordingCloud : CloudIo {
    val pushed = java.util.Collections.synchronizedList(mutableListOf<CloudBlob>())

    override suspend fun account(): CloudAccount = CloudAccount.AVAILABLE
    override suspend fun pull(cursor: ByteArray?, limit: UInt): CloudPage = CloudPage(emptyList(), byteArrayOf(1), false)
    override suspend fun push(blobs: List<CloudBlob>) { pushed.addAll(blobs) }
}

class CloudBridgeTest {
    @Test
    fun aLocalWriteReachesTheCloudAdapterThroughTheBridge() = bounded {
        val database = openDatabase(Config(null, null, 32u), CLOUD_MODELS, null)
        val cloud = RecordingCloud()
        val link = database.connectCloud(cloud)
        database.create("Note", """{"title":"hello"}""", "{}")
        link.syncNow()
        assertEquals(1, cloud.pushed.size)
        assertEquals(64, cloud.pushed.first().key.length)
        assertTrue(cloud.pushed.first().bytes.isNotEmpty())
        assertEquals(CloudState.Idle, link.state())
        link.stop()
    }
}
