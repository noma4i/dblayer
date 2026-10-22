package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Test

private const val CLOUD_MODELS = """[{"key":"Note","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"title","kind":{"type":"scalar","kind":"str"},"optional":true}],"storage":"local","cloud":["title"]}]"""

private class RecordingCloud : CloudIo {
    val pushed = java.util.Collections.synchronizedList(mutableListOf<CloudChange>())

    override suspend fun account(): CloudAccount = CloudAccount.AVAILABLE
    override suspend fun pull(cursor: ByteArray?, limit: UInt): CloudPage = CloudPage(emptyList(), byteArrayOf(1), false)
    override suspend fun push(changes: List<CloudChange>) { pushed.addAll(changes) }
}

class CloudBridgeTest {
    @Test
    fun aLocalWriteReachesTheCloudAdapterThroughTheBridge() = bounded {
        val database = Database.open(Config(null, null, 32u), CLOUD_MODELS, null)
        val cloud = RecordingCloud()
        val link = database.connectCloud(cloud)
        database.create("Note", """{"title":"hello"}""", "{}")
        link.syncNow()
        val titles = cloud.pushed.filter { it.field == "title" }
        assertEquals(1, titles.size)
        assertEquals(CloudValue.Text("hello"), titles.first().value)
        assertEquals(CloudState.Idle, link.state())
        link.stop()
    }
}
