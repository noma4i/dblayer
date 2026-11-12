package com.noma4i.dblayer

import com.noma4i.dblayer.models.modelsJSON
import org.junit.Assert.assertEquals
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35], shadows = [PulsingConnectivityManager::class])
class NetworkPathTest {
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
