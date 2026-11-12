package com.noma4i.dblayer

import android.content.Context
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

/** The reader and writer of media for the prepare step: it reads the metadata of a source and writes the result by the plan of the core, it decides nothing. */
internal class SystemPreparer(
    private val context: Context,
    private val videoFacts: (File) -> VideoFacts = ::readVideoFacts,
    private val decoderList: (String) -> List<DecoderAbility> = ::readDecoders,
    private val exporter: suspend (VideoExport) -> Unit = ::exportWithTransformer,
) : Preparer {
    private val running = ConcurrentHashMap<ULong, Job>()

    override suspend fun probe(path: String): ProbeOutcome = withContext(Dispatchers.IO) {
        try {
            val file = File(path)
            if (!file.isFile || !file.canRead()) ProbeOutcome.Failed("$path is not a readable file")
            else probeImage(file) ?: probeVideo(file)
        } catch (error: CancellationException) {
            throw error
        } catch (error: Exception) {
            ProbeOutcome.Failed(error.message ?: error.javaClass.simpleName)
        }
    }

    override suspend fun run(job: PrepareJob, progress: PrepareProgress): PrepareOutcome = coroutineScope {
        val work = async(Dispatchers.IO, start = CoroutineStart.LAZY) { write(job, progress) }
        running[job.attemptId] = work
        try {
            work.await()
        } catch (cancelled: CancellationException) {
            currentCoroutineContext().ensureActive()
            PrepareOutcome.Failed("the preparation was cancelled")
        } finally {
            running.remove(job.attemptId)
        }
    }

    override fun cancel(attemptId: ULong) {
        running[attemptId]?.cancel()
    }

    private fun probeVideo(file: File): ProbeOutcome {
        val facts = videoFacts(file)
        return ProbeOutcome.Video(facts.toSource(), readMetadataKeys(context, file))
    }

    private suspend fun write(job: PrepareJob, progress: PrepareProgress): PrepareOutcome {
        val destination = File(job.destination)
        var outcome: PrepareOutcome = PrepareOutcome.Failed("the preparation did not finish")
        try {
            outcome = when (val plan = job.plan) {
                is PreparePlan.Video -> writeVideo(File(job.source), destination, plan, progress)
                is PreparePlan.Image -> writeImage(File(job.source), destination, plan, progress)
            }
        } catch (error: CancellationException) {
            throw error
        } catch (error: Exception) {
            outcome = PrepareOutcome.Failed(error.message ?: error.javaClass.simpleName)
        } finally {
            if (outcome !is PrepareOutcome.Done) withContext(NonCancellable) { destination.delete() }
        }
        return outcome
    }

    private suspend fun writeVideo(source: File, destination: File, plan: PreparePlan.Video, progress: PrepareProgress): PrepareOutcome {
        val facts = videoFacts(source)
        val encode = plan.plan.reencode || plan.mirror
        if (encode) decoderRefusal(facts, decoderList(facts.mime))?.let { return PrepareOutcome.CannotDecode(it) }
        exporter(VideoExport(context, source, destination, transformSetup(plan, facts, encode)) { progress.report(it) })
        currentCoroutineContext().ensureActive()
        progress.report(1.0)
        return PrepareOutcome.Done(null, "video/mp4")
    }
}
