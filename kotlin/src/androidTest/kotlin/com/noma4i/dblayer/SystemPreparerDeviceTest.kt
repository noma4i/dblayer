package com.noma4i.dblayer

import android.graphics.BitmapFactory
import android.media.ExifInterface
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaMetadataRetriever
import android.util.Log
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class SystemPreparerDeviceTest {
    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private val preparer = SystemPreparer(context)

    private class Result(val mime: String, val width: Int, val height: Int, val frames: Int, val hasAudio: Boolean)

    private class Src(val width: Int, val height: Int, val turn: Int = 0, val aac: Boolean? = true, val location: Boolean = false)

    private class Cell(val name: String, val asset: String, val plan: PreparePlan, val mayFail: Boolean = false, val src: Src? = null, val check: (File, File) -> String)

    private fun planOf(plan: PreparePlan, probe: ProbeOutcome): PreparePlan =
        if (plan is PreparePlan.Video && plan.stripMetadata.isNotEmpty() && probe is ProbeOutcome.Video) plan.copy(stripMetadata = probe.metadataKeys) else plan

    private fun kindOf(plan: PreparePlan) = if (plan is PreparePlan.Video) PrepareKind.VIDEO else PrepareKind.IMAGE

    private fun probeRow(cell: Cell, outcome: ProbeOutcome): String {
        val expected = cell.src
        return when (outcome) {
            is ProbeOutcome.Video -> {
                val source = outcome.source
                if (expected != null) {
                    assertEquals("probe size", "${expected.width}x${expected.height}", "${source.width}x${source.height}")
                    assertEquals("probe turn", expected.turn, source.rotationDegrees)
                    if (expected.aac != null) assertEquals("probe aac", expected.aac, source.mp4H264Aac)
                    if (expected.location) assertTrue("probe keys ${outcome.metadataKeys}", "location" in outcome.metadataKeys)
                } else {
                    assertTrue("probe size ${source.width}x${source.height}", source.width > 0u && source.height > 0u)
                }
                "probe video ${source.width}x${source.height} turn=${source.rotationDegrees} keys=${outcome.metadataKeys}"
            }
            is ProbeOutcome.Image -> {
                if (expected != null) {
                    assertEquals("probe size", "${expected.width}x${expected.height}", "${outcome.width}x${outcome.height}")
                    assertEquals("probe exif", expected.turn, outcome.exifOrientation.toInt())
                }
                "probe image ${outcome.width}x${outcome.height} exif=${outcome.exifOrientation}"
            }
            is ProbeOutcome.Failed -> {
                assertTrue("probe failure has a reason", outcome.message.isNotBlank())
                "probe failed ${outcome.message}"
            }
        }
    }

    private fun video(reencode: Boolean, width: Int, height: Int, rotation: Int = 0, strip: List<String> = emptyList(), mirror: Boolean = false) =
        PreparePlan.Video(VideoPlan(reencode, width.toUInt(), height.toUInt(), 700_000u, 30u, rotation.toUInt()), strip, 128_000u, 2u, 44_100u, mirror)

    private fun image(width: Int, height: Int, turn: Int, format: ImageFormat) =
        PreparePlan.Image(
            PhotoPlan(PhotoTurn(turn.toUInt(), false), PixelSize(width.toUInt(), height.toUInt()), PixelRect(0u, 0u, width.toUInt(), height.toUInt()), false, PixelSize(width.toUInt(), height.toUInt()), false, 1u, PixelSize(width.toUInt(), height.toUInt()), width.toUInt(), 1u),
            0.9,
            format,
        )

    private fun copyAsset(asset: String): File =
        File(context.cacheDir, asset).also { file -> instrumentation.context.assets.open(asset).use { input -> file.outputStream().use { input.copyTo(it) } } }

    private fun read(file: File): Result {
        val extractor = MediaExtractor()
        val retriever = MediaMetadataRetriever()
        try {
            extractor.setDataSource(file.absolutePath)
            retriever.setDataSource(file.absolutePath)
            val formats = (0 until extractor.trackCount).map(extractor::getTrackFormat)
            val video = formats.first { it.getString(MediaFormat.KEY_MIME).orEmpty().startsWith("video/") }
            val turned = (if (video.containsKey(MediaFormat.KEY_ROTATION)) video.getInteger(MediaFormat.KEY_ROTATION) else 0) % 180 != 0
            val width = video.getInteger(MediaFormat.KEY_WIDTH)
            val height = video.getInteger(MediaFormat.KEY_HEIGHT)
            return Result(
                video.getString(MediaFormat.KEY_MIME).orEmpty(),
                if (turned) height else width,
                if (turned) width else height,
                retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_FRAME_COUNT)?.toIntOrNull() ?: 0,
                formats.any { it.getString(MediaFormat.KEY_MIME).orEmpty().startsWith("audio/") },
            )
        } finally {
            extractor.release()
            retriever.release()
        }
    }

    private fun dateOf(file: File): String? {
        val retriever = MediaMetadataRetriever()
        try {
            retriever.setDataSource(file.absolutePath)
            return retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_DATE)
        } finally {
            retriever.release()
        }
    }

    private fun videoRow(width: Int, height: Int, mime: String? = "video/avc", audio: Boolean? = true, keys: Boolean = false, frames: IntRange = 1..40): (File, File) -> String = { file, source ->
        val result = read(file)
        if (mime != null) assertEquals("codec", mime, result.mime)
        assertEquals("size", "${width}x$height", "${result.width}x${result.height}")
        if (audio != null) assertEquals("audio", audio, result.hasAudio)
        assertTrue("frames ${result.frames}", result.frames in frames)
        val found = readMetadataKeys(context, file)
        assertFalse("location kept: $found", found.any { it == "location" })
        val sourceDate = dateOf(source)
        assertTrue("source creation time kept: $sourceDate", sourceDate == null || sourceDate != dateOf(file))
        "${result.mime} ${result.width}x${result.height} frames=${result.frames} audio=${result.hasAudio}" + if (keys) " no-location" else ""
    }

    private fun imageRow(width: Int, height: Int, mime: String, alpha: Boolean = false): (File, File) -> String = { file, _ ->
        val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }.also { BitmapFactory.decodeFile(file.path, it) }
        assertEquals("format", mime, options.outMimeType)
        assertEquals("size", "${width}x$height", "${options.outWidth}x${options.outHeight}")
        val bitmap = BitmapFactory.decodeFile(file.path)
        assertEquals("alpha", alpha, bitmap.hasAlpha() && (0 until bitmap.width).any { x -> (0 until bitmap.height).any { y -> bitmap.getPixel(x, y) ushr 24 != 255 } })
        bitmap.recycle()
        if (mime == "image/jpeg") {
            val exif = ExifInterface(file.path)
            assertTrue("gps kept", !exif.getLatLong(FloatArray(2)))
            assertTrue("device kept", exif.getAttribute(ExifInterface.TAG_MAKE) == null && exif.getAttribute(ExifInterface.TAG_MODEL) == null)
            assertTrue("orientation kept", exif.getAttributeInt(ExifInterface.TAG_ORIENTATION, ExifInterface.ORIENTATION_NORMAL) !in 2..8)
        }
        "$mime ${options.outWidth}x${options.outHeight} alpha=$alpha no-gps no-device"
    }

    private val cells = listOf(
        Cell("H.264+AAC in limits with location (repack)", "h264-fit-location.mp4", video(false, 640, 360, strip = listOf("location")), src = Src(640, 360, location = true), check = videoRow(640, 360, keys = true)),
        Cell("H.264 above limits", "h264-large.mp4", video(true, 640, 360), src = Src(1280, 720), check = videoRow(640, 360)),
        Cell("HEVC", "hevc.mp4", video(true, 640, 360), mayFail = true, src = Src(640, 360, aac = false), check = videoRow(640, 360)),
        Cell("portrait with turn 270", "h264-turned.mp4", video(true, 360, 640, rotation = 270), src = Src(640, 360, turn = 270), check = videoRow(360, 640)),
        Cell("no audio track", "h264-silent.mp4", video(true, 320, 180), src = Src(640, 360, aac = true), check = videoRow(320, 180, audio = false)),
        Cell("mirror", "h264-fit-location.mp4", video(true, 640, 360, mirror = true), src = Src(640, 360), check = videoRow(640, 360)),
        Cell("4K source", "h264-4k.mp4", video(true, 1920, 1080), mayFail = true, src = Src(3840, 2160), check = videoRow(1920, 1080)),
        Cell("4K SDR device clip", "arg:clip4kSdr", video(true, 1920, 1080), mayFail = true, check = videoRow(1920, 1080, frames = 1..100000)),
        Cell("4K HDR10 device clip", "arg:clip4kHdr", video(true, 1920, 1080), mayFail = true, check = videoRow(1920, 1080, frames = 1..100000)),
        Cell("4K SDR device clip repack", "arg:clip4kSdr", video(false, 3840, 2160, strip = listOf("location")), check = videoRow(3840, 2160, mime = null, audio = null, frames = 1..100000)),
        Cell("1080p device clip", "arg:clip1080", video(true, 1920, 1080), check = videoRow(1920, 1080, audio = null, frames = 1..100000)),
        Cell("JPEG", "photo.jpg", image(800, 600, 0, ImageFormat.JPEG), src = Src(800, 600, turn = 1), check = imageRow(800, 600, "image/jpeg")),
        Cell("HEIC to JPEG", "photo.heic", image(800, 600, 0, ImageFormat.JPEG), mayFail = true, check = imageRow(800, 600, "image/jpeg")),
        Cell("PNG with alpha", "alpha.png", image(200, 100, 0, ImageFormat.PNG), src = Src(200, 100, turn = 1), check = imageRow(200, 100, "image/png", alpha = true)),
        Cell("EXIF turn 6 with GPS and device tags", "exif-turned.jpg", image(200, 400, 90, ImageFormat.JPEG), src = Src(400, 200, turn = 6), check = imageRow(200, 400, "image/jpeg")),
    )

    private class Quiet : PrepareProgress(NoHandle) {
        override fun report(fraction: Double) = Unit
    }

    @Test
    fun theSourceMatrixGivesAFileByThePlanOrAFailureWithAReason() = runBlocking<Unit> {
        val rows = mutableListOf<String>()
        val broken = mutableListOf<String>()
        val arguments = InstrumentationRegistry.getArguments()
        cells.forEachIndexed { index, cell ->
            val external = cell.asset.startsWith("arg:")
            val clip = if (external) arguments.getString(cell.asset.removePrefix("arg:")) else null
            if (external && clip == null) {
                Log.i("DBL_MATRIX", "${cell.name} -> SKIPPED no runner argument")
                return@forEachIndexed
            }
            val source = if (clip != null) File(clip) else copyAsset(cell.asset)
            val destination = File(context.cacheDir, "prepared-$index")
            val probe = preparer.probe(source.path, kindOf(cell.plan))
            val probed = try {
                probeRow(cell, probe)
            } catch (error: AssertionError) {
                broken += cell.name
                "probe WRONG ${error.message}"
            }
            if (probe is ProbeOutcome.Failed && !cell.mayFail) broken += cell.name
            val outcome = preparer.run(PrepareJob(index.toULong(), source.path, destination.path, planOf(cell.plan, probe)), Quiet())
            val row = when (outcome) {
                is PrepareOutcome.Done -> try {
                    "${cell.name} -> $probed | Done ${outcome.mime}: ${cell.check(destination, source)}"
                } catch (error: AssertionError) {
                    broken += cell.name
                    "${cell.name} -> $probed | WRONG ${error.message}"
                }
                is PrepareOutcome.CannotDecode -> "${cell.name} -> $probed | CannotDecode ${outcome.message}".also { if (!cell.mayFail || outcome.message.isBlank()) broken += cell.name }
                is PrepareOutcome.Failed -> "${cell.name} -> $probed | Failed ${outcome.message}".also { if (!cell.mayFail || outcome.message.isBlank()) broken += cell.name }
            }
            if (outcome !is PrepareOutcome.Done) assertFalse("a failed run leaves no file: ${cell.name}", destination.exists())
            Log.i("DBL_MATRIX", row)
            rows += row
            destination.delete()
            if (!external) source.delete()
        }
        assertTrue("cells outside the expectation: $broken\n${rows.joinToString("\n")}", broken.isEmpty())
    }

    @Test
    fun aFileOfAnotherKindGivesAFailureWithAReasonAndNoFile() = runBlocking<Unit> {
        val photo = copyAsset("photo.jpg")
        val clip = copyAsset("h264-silent.mp4")
        try {
            val image = preparer.probe(photo.path, PrepareKind.VIDEO)
            val video = preparer.probe(clip.path, PrepareKind.IMAGE)
            assertTrue("image probed as video: $image", image is ProbeOutcome.Failed && image.message.isNotBlank())
            assertTrue("video probed as image: $video", video is ProbeOutcome.Failed && video.message.isNotBlank())
        } finally {
            photo.delete()
            clip.delete()
        }
    }

    @Test
    fun cancelStopsTheRunAndRemovesThePartialFile() = runBlocking<Unit> {
        val source = copyAsset("h264-long.mp4")
        val destination = File(context.cacheDir, "cancelled.mp4")
        val started = AtomicBoolean(false)
        lateinit var outcome: PrepareOutcome
        val progress = object : PrepareProgress(NoHandle) {
            override fun report(fraction: Double) {
                if (started.compareAndSet(false, true)) preparer.cancel(7u)
            }
        }
        val job = launch { outcome = preparer.run(PrepareJob(7u, source.path, destination.path, video(true, 1280, 720)), progress) }
        job.join()
        source.delete()
        assertTrue("the export had to report before the cancel", started.get())
        assertTrue(outcome.toString(), outcome is PrepareOutcome.Failed)
        assertFalse(destination.exists())
    }
}
