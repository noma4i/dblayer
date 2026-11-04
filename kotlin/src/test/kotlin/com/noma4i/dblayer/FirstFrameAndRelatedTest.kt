package com.noma4i.dblayer

import com.noma4i.dblayer.models.Presence
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Test

private class FirstFrameSink(private val caller: Thread) : ValueSink {
    class Frame(val onCaller: Boolean, val insideSubscribe: Boolean, val ids: List<String>)

    @Volatile var inside = false
    @Volatile var first: Frame? = null

    override fun delivered(delivery: StreamDelivery) {
        val value = delivery.value as? StreamValue.Snapshot ?: return
        if (first == null) first = Frame(Thread.currentThread() === caller, inside, value.rows.map { it.id })
    }
}

/** Spec B, first frame: the cached rows come with the first delivery, read off the calling thread. Related queries: the target rows of a link follow a change of the target and of the link. */
class FirstFrameAndRelatedTest {
    private fun names(projected: Projected<User>) = projected.projection.value.rows.map { it.name }

    /** Requirement: a stream opened over cached rows delivers them in its first frame, not inside the opening call and not on the calling thread. Observable: the thread flag, the flag of the opening call and the ids of the first snapshot. */
    @Test
    fun theFirstFrameOfAWarmStreamArrivesOffTheCallingThreadWithTheCachedRows() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, MockTransport())
        val ann = User.create(name = "Ann")
        val seats = listOf(Presence.create(userId = ann.id, room = "r"), Presence.create(userId = ann.id, room = "r"))
        val scope = AccountScope.current()
        val sink = FirstFrameSink(Thread.currentThread())
        try {
            sink.inside = true
            val stream = DBlayer.database.subscribe(scope.epoch, StreamRequest.Query(Presence.where(Condition.equal("room", jsonValue("r"))).json), sink)
            sink.inside = false
            try {
                eventually("the first frame arrives") { sink.first != null }
                val frame = sink.first
                assertNotNull(frame)
                assertFalse("the first frame was delivered on the calling thread", frame!!.onCaller)
                assertFalse("the first frame was delivered inside the opening call", frame.insideSubscribe)
                assertEquals("the first frame lacked the cached rows: ${frame.ids}", seats.map { it.id }.toSet(), frame.ids.toSet())
            } finally {
                DBlayer.database.cancelStream(stream)
            }
        } finally {
            DBlayer.close()
        }
    }

    /** Requirement: a related query returns the target rows of the link, and follows a change of a target row and a change of the link. Observable: the names the projection shows after each step. */
    @Test
    fun aRelatedQueryReturnsTheLinkedRowsAndFollowsATargetChangeAndALinkChange() = bounded {
        reopen(Config(null, null, 32u), modelsJSON, MockTransport())
        val screen = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val ann = User.create(name = "Ann")
            val bob = User.create(name = "Bob")
            val seat = Presence.create(userId = ann.id, room = "r")
            val related = Projected(AccountScope.current().related(Presence.where(Condition.equal("room", jsonValue("r"))), Presence.Relations.user), User, screen)
            related.loaded()

            eventually("the linked row shows") { names(related) == listOf("Ann") }

            ann.update(name = "Anna")
            eventually("a change of the target shows") { names(related) == listOf("Anna") }

            seat.update(userId = bob.id)
            eventually("a change of the link moves the target") { names(related) == listOf("Bob") }

            Presence.create(userId = ann.id, room = "r")
            eventually("a second link adds its target") { names(related).let { it.size == 2 && it.toSet() == setOf("Anna", "Bob") } }
        } finally {
            screen.cancel()
            DBlayer.close()
        }
    }
}
