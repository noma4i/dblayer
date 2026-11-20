package com.noma4i.dblayer

import java.io.File

/** A file producer for tests, without codecs. */
class ScriptedPreparer(
    private val outcome: PrepareOutcome = PrepareOutcome.Done(null, null),
    private val facts: ProbeOutcome = DEFAULT_FACTS,
) : Preparer {
    override suspend fun probe(path: String, descriptor: ByteArray?, mime: String?): ProbeOutcome = facts

    override suspend fun run(job: PrepareJob, progress: PrepareProgress): PrepareOutcome {
        progress.report(0.5)
        if (outcome !is PrepareOutcome.Done) return outcome
        File(job.source).copyTo(File(job.destination), overwrite = true)
        progress.report(1.0)
        return outcome
    }

    override fun cancel(attemptId: ULong) {}

    override fun importDescriptor(saved: SavedPreparation): PrepareImportOutcome =
        PrepareImportOutcome.Failed("no saved preparation was configured")

    companion object {
        val DEFAULT_FACTS: ProbeOutcome = ProbeOutcome.File(1280u, 720u, null)
    }
}
