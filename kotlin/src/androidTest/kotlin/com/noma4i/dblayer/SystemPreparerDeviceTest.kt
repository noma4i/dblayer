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

    private class Cell(val name: String, val asset: String, val plan: PreparePlan, val mayFail: Boolean = false, val check: (File) -> String)

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

    private fun videoRow(width: Int, height: Int, mime: String? = "video/avc", audio: Boolean? = true, keys: Boolean = false, frames: IntRange = 1..40): (File) -> String = { file ->
        val result = read(file)
        if (mime != null) assertEquals("codec", mime, result.mime)
        assertEquals("size", "${width}x$height", "${result.width}x${result.height}")
        if (audio != null) assertEquals("audio", audio, result.hasAudio)
        assertTrue("frames ${result.frames}", result.frames in frames)
        val found = readMetadataKeys(context, file)
        assertFalse("location kept: $found", found.any { it == "location" || it == "creation_time" })
        "${result.mime} ${result.width}x${result.height} frames=${result.frames} audio=${result.hasAudio}" + if (keys) " no-location" else ""
    }

    private fun imageRow(width: Int, height: Int, mime: String, alpha: Boolean = false): (File) -> String = { file ->
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
            assertEquals("orientation kept", ExifInterface.ORIENTATION_NORMAL, exif.getAttributeInt(ExifInterface.TAG_ORIENTATION, ExifInterface.ORIENTATION_NORMAL))
        }
        "$mime ${options.outWidth}x${options.outHeight} alpha=$alpha no-gps no-device"
    }

    private val cells = listOf(
        Cell("H.264+AAC in limits with location (repack)", "h264-fit-location.mp4", video(false, 640, 360, strip = listOf("location")), check = videoRow(640, 360, keys = true)),
        Cell("H.264 above limits", "h264-large.mp4", video(true, 640, 360), check = videoRow(640, 360)),
        Cell("HEVC", "hevc.mp4", video(true, 640, 360), mayFail = true, check = videoRow(640, 360)),
        Cell("portrait with turn 90", "h264-turned.mp4", video(true, 360, 640, rotation = 90), check = videoRow(360, 640)),
        Cell("no audio track", "h264-silent.mp4", video(true, 320, 180), check = videoRow(320, 180, audio = false)),
        Cell("mirror", "h264-fit-location.mp4", video(true, 640, 360, mirror = true), check = videoRow(640, 360)),
        Cell("4K source", "h264-4k.mp4", video(true, 1920, 1080), mayFail = true, check = videoRow(1920, 1080)),
        Cell("4K SDR device clip", "arg:clip4kSdr", video(true, 1920, 1080), mayFail = true, check = videoRow(1920, 1080, frames = 1..100000)),
        Cell("4K HDR10 device clip", "arg:clip4kHdr", video(true, 1920, 1080), mayFail = true, check = videoRow(1920, 1080, frames = 1..100000)),
        Cell("4K SDR device clip repack", "arg:clip4kSdr", video(false, 3840, 2160, strip = listOf("location")), check = videoRow(3840, 2160, mime = null, audio = null, frames = 1..100000)),
        Cell("1080p device clip", "arg:clip1080", video(true, 1920, 1080), check = videoRow(1920, 1080, audio = null, frames = 1..100000)),
        Cell("JPEG", "photo.jpg", image(800, 600, 0, ImageFormat.JPEG), check = imageRow(800, 600, "image/jpeg")),
        Cell("HEIC to JPEG", "photo.heic", image(800, 600, 0, ImageFormat.JPEG), mayFail = true, check = imageRow(800, 600, "image/jpeg")),
        Cell("PNG with alpha", "alpha.png", image(200, 100, 0, ImageFormat.PNG), check = imageRow(200, 100, "image/png", alpha = true)),
        Cell("EXIF turn 6 with GPS and device tags", "exif-turned.jpg", image(200, 400, 90, ImageFormat.JPEG), check = imageRow(200, 400, "image/jpeg")),
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
            val outcome = preparer.run(PrepareJob(index.toULong(), source.path, destination.path, cell.plan), Quiet())
            val row = when (outcome) {
                is PrepareOutcome.Done -> try {
                    "${cell.name} -> Done ${outcome.mime}: ${cell.check(destination)}"
                } catch (error: AssertionError) {
                    broken += cell.name
                    "${cell.name} -> WRONG ${error.message}"
                }
                is PrepareOutcome.CannotDecode -> "${cell.name} -> CannotDecode ${outcome.message}".also { if (!cell.mayFail || outcome.message.isBlank()) broken += cell.name }
                is PrepareOutcome.Failed -> "${cell.name} -> Failed ${outcome.message}".also { if (!cell.mayFail || outcome.message.isBlank()) broken += cell.name }
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
