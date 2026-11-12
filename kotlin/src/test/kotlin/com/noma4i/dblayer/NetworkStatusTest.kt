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
        assertEquals(false, one.next()?.online)
        assertEquals(false, two.next()?.online)
        osNetwork(true)
        assertEquals(true, one.next()?.online)
        assertEquals(true, two.next()?.online)
        assertEquals(true, status.isOnline)
        osNetwork(false)
        assertEquals(false, one.next()?.online)
        assertEquals(false, NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication()).isOnline)
    }

    /** bridge_network_path: the transport and the change of the `Network` object reach `path`, `isOnline` and `flow` as the same values. */
    @Test
    fun bridge_network_path_kotlinCarriesThePathAndTheChangeOfNetwork() = bounded {
        openSignedOffline(Config(null, null), modelsJSON)
        val status = NetworkStatus(org.robolectric.RuntimeEnvironment.getApplication())
        osNetwork(true, android.net.NetworkCapabilities.TRANSPORT_WIFI)
        val reader = status.subscribe()
        val wifi = reader.next()!!
        assertEquals(NetworkTransport.WIFI, wifi.transport)
        assertEquals(wifi, status.path)
        osNetwork(true, android.net.NetworkCapabilities.TRANSPORT_CELLULAR, org.robolectric.shadows.ShadowNetwork.newInstance(77))
        val cellular = reader.next()!!
        assertEquals(Triple(true, NetworkTransport.CELLULAR, true), Triple(cellular.online, cellular.transport, cellular.network != wifi.network))
        assertEquals(true, status.isOnline)
    }
}
