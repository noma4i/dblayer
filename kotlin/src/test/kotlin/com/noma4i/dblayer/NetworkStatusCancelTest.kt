package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35], shadows = [PulsingConnectivityManager::class])
class NetworkStatusCancelTest {
    /** net_status_cancel_one_reader: cancelling one collector ends only that collector and the other one still gets the next value. */
    @Test
    fun net_status_kotlinCancellingOneCollectorEndsOnlyThatCollector() = bounded {
        openSignedOffline(Config(null), modelsJSON)
        val status = NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication())
        osNetwork(false)
        val seen = kotlinx.coroutines.channels.Channel<NetworkPath>(kotlinx.coroutines.channels.Channel.UNLIMITED)
        val ending = launch { status.flow.collect { seen.send(it) } }
        assertEquals(false, seen.receive().online)
        val kept = status.subscribe()
        assertEquals(false, kept.next()?.online)
        ending.cancel()
        awaitWithin("the cancelled collector ends") { ending.join() }
        osNetwork(true)
        assertEquals(true, kept.next()?.online)
        assertEquals(true, status.isOnline)
    }
}
