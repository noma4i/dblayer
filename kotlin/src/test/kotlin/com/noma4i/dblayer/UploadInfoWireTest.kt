package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Test

class UploadInfoWireTest {
    @Test
    fun uploadInfoCarriesLocalUrlAndProcessingOfTheWireUpload() {
        val wire = WireUpload(WireUploadStatus.UPLOADING, "put", 1uL, 4uL, 25.0, 0.75, null, 0u, null, "file:///tmp/clip.mov", WireProcessing(0.5))
        val info = UploadInfo.of(wire)
        assertEquals("file:///tmp/clip.mov", info.localUrl)
        assertEquals(UploadProcessing(0.5), info.processing)
        assertEquals(0.75, info.batchProgress, 0.0)
    }
}
