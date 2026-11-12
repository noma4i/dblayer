package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35], shadows = [PulsingConnectivityManager::class])
class NetworkStatusTest {
    /** net_status_first_value, net_status_change: the platform value is the public value and each collector gets a change once. */
    @Test
    fun net_status_kotlinCollectorsGetTheFirstValueAndEachChange() = bounded {
        openSignedOffline(Config(null, null), modelsJSON)
        val status = NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication())
        osNetwork(false)
        assertEquals(false, status.isOnline)
        val one = status.subscribe()
        val two = status.subscribe()
        assertEquals(false, one.next())
        assertEquals(false, two.next())
        osNetwork(true)
        assertEquals(true, one.next())
        assertEquals(true, two.next())
        assertEquals(true, status.isOnline)
        osNetwork(false)
        assertEquals(false, one.next())
        assertEquals(false, NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication()).isOnline)
    }
}
