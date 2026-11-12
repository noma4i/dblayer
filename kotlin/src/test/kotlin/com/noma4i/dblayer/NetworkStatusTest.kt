package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35], shadows = [PulsingConnectivityManager::class])
class NetworkStatusTest {
    /** net_status_first_value, net_status_change: the platform value is the public value and each collector gets a change once. */
    @Test
    fun net_status_kotlinCollectorsGetTheFirstValueAndEachChange() = bounded {
        openSignedOffline(Config(null, null), modelsJSON)
        DBlayer.database.setOnline(false)
        val status = NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication())
        assertEquals(false, status.isOnline)
        val one = status.subscribe()
        val two = status.subscribe()
        assertEquals(false, one.next())
        assertEquals(false, two.next())
        DBlayer.database.setOnline(true)
        assertEquals(true, one.next())
        assertEquals(true, two.next())
        assertEquals(true, status.isOnline)
        DBlayer.database.setOnline(false)
        assertEquals(false, one.next())
        assertEquals(false, NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication()).isOnline)
    }

    /** net_status_same_value_silent, net_status_ignores_server_and_session: the same value again and a server answer wake nobody, so the first frame after them is the next real change. */
    @Test
    fun net_status_kotlinAServerAnswerAndARepeatedValueGiveNoFrame() = bounded {
        openSignedOffline(Config(null, null), modelsJSON)
        DBlayer.database.setOnline(true)
        val status = NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication())
        val reader = status.subscribe()
        assertEquals(true, reader.next())
        DBlayer.database.setOnline(true)
        DBlayer.database.setOnline(true)
        Moment.create(text = "answered", account = AccountScope.current()).optimisticValue!!
        assertEquals(true, status.isOnline)
        DBlayer.database.setOnline(false)
        assertEquals(false, reader.next())
        assertEquals(false, status.isOnline)
    }

    /** net_status_reader_gets_current: a collector that starts late gets the newest value first. */
    @Test
    fun net_status_kotlinALateCollectorGetsTheNewestValueFirst() = bounded {
        openSignedOffline(Config(null, null), modelsJSON)
        DBlayer.database.setOnline(true)
        DBlayer.database.setOnline(false)
        assertEquals(false, NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication()).flow.first())
    }

    /** net_status_cancel_one_reader: cancelling one collector ends only that collector and the other one still gets the next value. */
    @Test
    fun net_status_kotlinCancellingOneCollectorEndsOnlyThatCollector() = bounded {
        openSignedOffline(Config(null, null), modelsJSON)
        DBlayer.database.setOnline(false)
        val status = NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication())
        val seen = kotlinx.coroutines.channels.Channel<Boolean>(kotlinx.coroutines.channels.Channel.UNLIMITED)
        val ending = launch { status.flow.collect { seen.send(it) } }
        assertEquals(false, seen.receive())
        val kept = status.subscribe()
        assertEquals(false, kept.next())
        ending.cancel()
        awaitWithin("the cancelled collector ends") { ending.join() }
        DBlayer.database.setOnline(true)
        assertEquals(true, kept.next())
        assertEquals(true, status.isOnline)
    }

    /** net_status_observer_starts_once: the second and third `NetworkStatus(context)` start no new observer of the platform. */
    @Test
    fun net_status_kotlinFurtherStatusesStartNoSecondObserver() = bounded {
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        val manager = context.getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as android.net.ConnectivityManager
        val shadow = org.robolectric.Shadows.shadowOf(manager)
        NetworkStatus(context)
        val registered = shadow.networkCallbacks.size
        NetworkStatus(context)
        NetworkStatus(context)
        assertEquals(registered, shadow.networkCallbacks.size)
    }
}
