package com.noma4i.dblayer

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import org.junit.Assert.assertEquals
import org.junit.Test

class FailureValuesTest {
    /** The functions of the test fixtures build a refusal, a sync value and an upload value, and each reads as the row of a failed operation does. */
    @Test
    fun bridge_failure_values_in_tests_kotlinBuildsTheValuesFromFields() {
        val failure = operationFailure("Invalid", "the cell is empty", "refused")
        assertEquals(listOf<Any?>("Invalid", "the cell is empty", "refused", null, null), listOf(failure.kind, failure.reason, failure.message, failure.sessionCode, failure.requestSize))
        val sync = syncInfo(SyncState.Failed, failure)
        assertEquals(listOf<Any?>(SyncState.Failed, failure, emptyList<String>(), 0), listOf(sync.state, sync.error, sync.actions, sync.attempts))
        val upload = uploadInfo(UploadStatus.Failed, 0.4, failure)
        assertEquals(listOf<Any?>(UploadStatus.Failed, 0.4, failure), listOf(upload.status, upload.progress, upload.error))
        assertEquals(sync, dbJson.decodeFromString<SyncInfo>(dbJson.encodeToString(sync)))
    }
}
