package com.noma4i.dblayer

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Matrix
import android.graphics.Paint
import android.graphics.Rect
import android.media.ExifInterface
import java.io.File
import kotlin.math.roundToInt
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

internal fun probeImage(file: File): ProbeOutcome? {
    val bounds = BitmapFactory.Options().apply { inJustDecodeBounds = true }.also { BitmapFactory.decodeFile(file.path, it) }
    if (bounds.outWidth <= 0 || bounds.outHeight <= 0) return null
    val tag = ExifInterface(file.path).getAttributeInt(ExifInterface.TAG_ORIENTATION, ExifInterface.ORIENTATION_NORMAL).takeIf { it in 1..8 } ?: ExifInterface.ORIENTATION_NORMAL
    val format = if (bounds.outMimeType == "image/png") ImageFormat.PNG else ImageFormat.JPEG
    return ProbeOutcome.Image(bounds.outWidth.toUInt(), bounds.outHeight.toUInt(), tag.toUInt(), format)
}

internal suspend fun writeImage(source: File, destination: File, prepare: PreparePlan.Image, progress: PrepareProgress): PrepareOutcome {
    val plan = prepare.plan
    val decoded = BitmapFactory.decodeFile(source.path, BitmapFactory.Options().apply { inSampleSize = plan.sample.toInt() })
        ?: return PrepareOutcome.Failed("${source.path} is not an image")
    currentCoroutineContext().ensureActive()
    val turned = Bitmap.createBitmap(decoded, 0, 0, decoded.width, decoded.height, turnMatrix(plan.turn), true)
    val drawn = if (plan.needsDraw) redraw(turned, plan) else turned
    val flat = if (prepare.format == ImageFormat.JPEG && drawn.hasAlpha()) onWhite(drawn) else drawn
    try {
        currentCoroutineContext().ensureActive()
        val written = destination.outputStream().use {
            if (prepare.format == ImageFormat.PNG) flat.compress(Bitmap.CompressFormat.PNG, 100, it)
            else flat.compress(Bitmap.CompressFormat.JPEG, (prepare.quality * 100).roundToInt(), it)
        }
        if (!written) return PrepareOutcome.Failed("the image does not encode")
    } finally {
        if (flat !== drawn) flat.recycle()
        if (drawn !== turned) drawn.recycle()
        if (turned !== decoded) turned.recycle()
        decoded.recycle()
    }
    progress.report(1.0)
    return PrepareOutcome.Done(null, if (prepare.format == ImageFormat.PNG) "image/png" else "image/jpeg")
}

/** The crop of the plan, moved to the sampled size of [upright], drawn into the output size and flipped when the plan asks for the mirror. */
internal fun redraw(upright: Bitmap, plan: PhotoPlan): Bitmap {
    val width = plan.output.width.toInt()
    val height = plan.output.height.toInt()
    val result = Bitmap.createBitmap(width, height, Bitmap.Config.ARGB_8888)
    val canvas = Canvas(result)
    if (plan.mirror) canvas.scale(-1f, 1f, width / 2f, 0f)
    val across = upright.width.toDouble() / plan.upright.width.toInt()
    val down = upright.height.toDouble() / plan.upright.height.toInt()
    val left = (plan.crop.x.toInt() * across).roundToInt().coerceIn(0, upright.width - 1)
    val top = (plan.crop.y.toInt() * down).roundToInt().coerceIn(0, upright.height - 1)
    val area = Rect(
        left,
        top,
        ((plan.crop.x + plan.crop.width).toInt() * across).roundToInt().coerceIn(left + 1, upright.width),
        ((plan.crop.y + plan.crop.height).toInt() * down).roundToInt().coerceIn(top + 1, upright.height),
    )
    canvas.drawBitmap(upright, area, Rect(0, 0, width, height), Paint(Paint.FILTER_BITMAP_FLAG))
    return result
}

internal fun turnMatrix(turn: PhotoTurn): Matrix =
    Matrix().apply {
        postRotate(turn.rotationDegrees.toFloat())
        if (turn.flipHorizontal) postScale(-1f, 1f)
    }

private fun onWhite(source: Bitmap): Bitmap {
    val result = Bitmap.createBitmap(source.width, source.height, Bitmap.Config.ARGB_8888)
    Canvas(result).apply {
        drawColor(Color.WHITE)
        drawBitmap(source, 0f, 0f, null)
    }
    return result
}
