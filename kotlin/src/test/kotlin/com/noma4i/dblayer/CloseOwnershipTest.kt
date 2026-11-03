package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

class CloseOwnershipTest {
    @Test
    fun detachingTheSinkStopsDelivery() {
        val seen = CopyOnWriteArrayList<Boolean>()
        val relay = OnlineRelay { }
        relay.attach { seen += it }
        val before = seen.size
        relay.detach()
        relay.set(true)
        assertEquals(before, seen.size)
    }

    @Test
    fun closingTheDatabaseEndsAWaitingChangesFlow() = bounded {
        reopenSigned(Config(null, null, 8u), modelsJSON)
        Moment.send(AccountScope.current(), text = "kept").row
        val received = CopyOnWriteArrayList<Any>()
        val collecting = launch(Dispatchers.Default) { Moment.recent.live.flow.collect { received.add(it) } }
        eventually("the initial delivery") { received.isNotEmpty() }
        DBlayer.close()
        awaitWithin("the changes flow to end after the close") { collecting.join() }
    }
}
