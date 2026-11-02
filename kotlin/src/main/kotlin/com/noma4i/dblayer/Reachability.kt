package com.noma4i.dblayer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities

internal object Reachability {
    private val connectivity = Connectivity(false)
    private val relay = OnlineRelay { connectivity.setOnline(it) }
    private val lock = Any()
    private var manager: ConnectivityManager? = null

    fun start(context: Context): Connectivity {
        val made = synchronized(lock) {
            if (manager != null) return connectivity
            val service = context.applicationContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
            manager = service
            service.registerDefaultNetworkCallback(object : ConnectivityManager.NetworkCallback() {
                override fun onAvailable(network: Network) = relay.set(isOnline(service))
                override fun onLost(network: Network) = relay.set(isOnline(service))
                override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) =
                    relay.set(capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED))
            })
            service
        }
        relay.set(isOnline(made))
        return connectivity
    }

    fun connect(sink: OnlineSink) = relay.attach { sink.setOnline(it) }

    fun disconnect() = relay.detach()

    private fun isOnline(manager: ConnectivityManager): Boolean {
        val capabilities = manager.getNetworkCapabilities(manager.activeNetwork) ?: return false
        return capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)
    }
}
