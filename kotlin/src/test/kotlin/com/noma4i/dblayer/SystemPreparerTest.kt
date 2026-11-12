package com.noma4i.dblayer

import java.io.File
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35])
class SystemPreparerTest {
    private val context = org.robolectric.RuntimeEnvironment.getApplication()

    private class Quiet : PrepareProgress(NoHandle) {
        override fun report(fraction: Double) = Unit
    }

    private fun decoder(maxWidth: Int, maxHeight: Int) = DecoderAbility({ width, height -> width <= maxWidth && height <= maxHeight }, setOf(8), maxWidth, maxHeight)

    private fun source(): File = File.createTempFile("clip", ".mp4").apply { writeBytes(byteArrayOf(0)); deleteOnExit() }

    private fun destination(): File = File.createTempFile("prepared", ".mp4").apply { delete() }

    private fun job(plan: PreparePlan.Video, source: File, destination: File, attempt: ULong = 1u) = PrepareJob(attempt, source.path, destination.path, plan)

    private fun plan(reencode: Boolean, strip: List<String> = emptyList()) =
        PreparePlan.Video(VideoPlan(reencode, 1920u, 1080u, 3_000_000u, 30u, 0u), strip, 128_000u, 2u, 44_100u, false)

    private fun facts4k() = VideoFacts(3840, 2160, 0, 30f, 2_000_000, 2, true, "video/avc", 8)

    /** A source above every decoder is refused with the decoder maximum before the export, a source that is only repackaged needs no decoder. */
    @Test
    fun theDecoderIsCheckedOnlyWhenTheVideoIsEncoded() = bounded<Unit> {
        val exports = CopyOnWriteArrayList<VideoExport>()
        val preparer = SystemPreparer(context, { facts4k() }, { listOf(decoder(2560, 1440)) }, { exports += it; it.output.writeBytes(byteArrayOf(1)) })
        val refused = destination()

        val refusal = preparer.run(job(plan(true), source(), refused), Quiet())

        assertEquals("the device decodes video/avc up to 2560x1440, the video is 3840x2160", (refusal as PrepareOutcome.CannotDecode).message)
        assertTrue(exports.isEmpty())
        assertFalse(refused.exists())

        val repacked = destination()
        val done = preparer.run(job(plan(false, listOf("location")), source(), repacked), Quiet())

        assertEquals(PrepareOutcome.Done(null, "video/mp4"), done)
        assertNull(exports.single().setup.videoMime)
        assertTrue(repacked.exists())
        repacked.delete()
    }

    /** `cancel` of the attempt stops the export and no partial file stays. */
    @Test
    fun cancelStopsTheExportAndRemovesThePartialFile() = bounded<Unit> {
        val started = CompletableDeferred<Unit>()
        val output = destination()
        val preparer = SystemPreparer(context, { facts4k() }, { listOf(decoder(Int.MAX_VALUE, Int.MAX_VALUE)) }, {
            it.output.writeBytes(byteArrayOf(1))
            started.complete(Unit)
            awaitCancellation()
        })

        val running = async { preparer.run(job(plan(true), source(), output, 9u), Quiet()) }
        withTimeout(10_000) { started.await() }
        preparer.cancel(9u)

        assertTrue(withTimeout(10_000) { running.await() } is PrepareOutcome.Failed)
        assertFalse(output.exists())
    }
}
