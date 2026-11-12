package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.firstOrNull
import org.junit.Assert.assertNotNull
import org.junit.Test

private const val CREATE_MOMENT_OPERATION = """[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }"}]"""

@OptIn(DBlayerTestingApi::class)
class DeferredSourceRestartTest {
    @Test
    fun aDeferredSourceReadsMissingFileAfterARestart() = bounded {
        val directory = kotlin.io.path.createTempDirectory().toFile()
        try {
            closeForCleanup()
            val path = directory.resolve("restart.sqlite").absolutePath
            val transport = MockTransport()
            openSignedClocked(modelsJSON, transport, CREATE_MOMENT_OPERATION, path = path)
            AccountScope.current().upload(Moment, "absent", "upload_clip", UploadRequest.pending())
            openSignedClocked(modelsJSON, transport, CREATE_MOMENT_OPERATION, path = path)
            val scope = AccountScope.current()
            val staged = StagedUpload(scope.database, scope.epoch, Moment, "absent", "upload_clip")
            val failed = scope.uploadStatus(staged).uploadStatus.firstOrNull { it is TransferItem.Progress && it.progress.state == "failed" && it.progress.error?.contains("missing_file") == true }
            assertNotNull(failed)
        } finally {
            closeForCleanup()
            directory.deleteRecursively()
        }
    }
}
