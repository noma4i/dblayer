package com.noma4i.dblayer

import java.io.File

/** A preparer without codecs for the stand: the probe answers the kind the settings name, `run` reports half and all of the work and copies the source to the result. */
class ScriptedPreparer(
    private val outcome: PrepareOutcome = PrepareOutcome.Done(null, null),
    private val video: ProbeOutcome = DEFAULT_VIDEO,
) : Preparer {
    override suspend fun probe(path: String, kind: PrepareKind): ProbeOutcome = when (kind) {
        PrepareKind.VIDEO -> video
        PrepareKind.IMAGE -> ProbeOutcome.Image(1080u, 1920u, 1u, ImageFormat.JPEG)
    }

    override suspend fun run(job: PrepareJob, progress: PrepareProgress): PrepareOutcome {
        progress.report(0.5)
        if (outcome !is PrepareOutcome.Done) return outcome
        File(job.source).copyTo(File(job.destination), overwrite = true)
        progress.report(1.0)
        return outcome
    }

    override fun cancel(attemptId: ULong) {}

    companion object {
        /** A 1280x720 MP4 of H.264 and AAC at 30 frames per second without private metadata keys. */
        val DEFAULT_VIDEO: ProbeOutcome = ProbeOutcome.Video(VideoSource(1280u, 720u, 0, 30f, 1_000_000u, true), emptyList())
    }
}
