package com.noma4i.dblayer

import com.noma4i.dblayer.models.Chat
import com.noma4i.dblayer.models.Presence
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Test

/** Spec B, first frame: the cached rows come with the first value, read off the calling thread. Related queries: a stream over a server relation follows a change of its rows and of the link. */
class FirstFrameAndRelatedTest {

    /** Requirement: a stream opened over cached rows delivers them in its very first value, which the reader takes off the calling thread. Observable: the kind, the rows and the revision of the first value, and the thread that took it. */
    @Test
    fun theFirstFrameOfAWarmStreamArrivesOffTheCallingThreadWithTheCachedRows() = bounded {
        openSigned(Config(null), modelsJSON, MockTransport())
        val ann = User.create(name = "Ann").optimisticValue!!
        val seats = listOf(Presence.create(userId = ann.id, room = "r").optimisticValue!!, Presence.create(userId = ann.id, room = "r").optimisticValue!!)
        val stream = AccountScope.current().stream(Presence.where(Condition.equal("room", jsonValue("r"))))
        val caller = Thread.currentThread()
        try {
            val (onCaller, first) = withContext(Dispatchers.Default) { (Thread.currentThread() === caller) to stream.values.first() }
            assertFalse("the first value was taken on the calling thread", onCaller)
            val snapshot = first as? ReadValue.Snapshot
            assertNotNull("the first value was not a snapshot with a revision", snapshot)
            assertEquals("the first value lacked the cached rows: ${snapshot?.rows?.map { it.id }}", seats.map { it.id }.toSet(), snapshot!!.rows.map { it.id }.toSet())
        } finally {
            stream.cancel()
            closeForCleanup()
        }
    }
}
