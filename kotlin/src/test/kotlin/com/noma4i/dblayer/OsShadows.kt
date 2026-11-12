package com.noma4i.dblayer

import android.app.Service
import android.net.ConnectivityManager
import org.robolectric.annotation.Implementation
import org.robolectric.annotation.Implements
import org.robolectric.shadows.ShadowConnectivityManager
import org.robolectric.shadows.ShadowService

/** The OS side of "the core registered its network callback": the shadow of the system service pulses the tests after it recorded the registration. */
@Implements(ConnectivityManager::class)
class PulsingConnectivityManager : ShadowConnectivityManager() {
    @Implementation
    override fun registerDefaultNetworkCallback(callback: ConnectivityManager.NetworkCallback) {
        super.registerDefaultNetworkCallback(callback)
        TestPulse.pulse()
    }
}

/** The OS side of "the service stopped itself": the shadow pulses the tests after it recorded `stopSelf`. */
@Implements(Service::class)
class PulsingService : ShadowService() {
    @Implementation
    override fun stopSelf() {
        super.stopSelf()
        TestPulse.pulse()
    }
}

/** The OS value of the network: it goes through the one callback the status of the process registered. */
internal fun osNetwork(online: Boolean, transport: Int? = null, network: android.net.Network? = null) {
    val manager = org.robolectric.RuntimeEnvironment.getApplication().getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as ConnectivityManager
    val network = network ?: manager.activeNetwork!!
    val capabilities = org.robolectric.shadows.ShadowNetworkCapabilities.newInstance()
    if (transport != null) org.robolectric.Shadows.shadowOf(capabilities).addTransportType(transport)
    if (online) org.robolectric.Shadows.shadowOf(capabilities).addCapability(android.net.NetworkCapabilities.NET_CAPABILITY_VALIDATED)
    val shadow = org.robolectric.Shadows.shadowOf(manager)
    shadow.setNetworkCapabilities(network, capabilities)
    shadow.networkCallbacks.single().onCapabilitiesChanged(network, capabilities)
}
