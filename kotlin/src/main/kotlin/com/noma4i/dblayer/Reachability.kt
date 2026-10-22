package com.noma4i.dblayer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities

internal object Reachability {
    private val connectivity = Connectivity(false)
    private var manager: ConnectivityManager? = null
    private var sink: OnlineSink? = null
    private var online = false

    @Synchronized
    fun start(context: Context): Connectivity {
        if (manager != null) return connectivity
        val manager = context.applicationContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        this.manager = manager
        manager.registerDefaultNetworkCallback(object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) = changed(isOnline(manager))
            override fun onLost(network: Network) = changed(isOnline(manager))
            override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) =
                changed(capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED))
        })
        changed(isOnline(manager))
        return connectivity
    }

    @Synchronized
    fun connect(sink: OnlineSink) {
        this.sink = sink
        sink.setOnline(online)
    }

    @Synchronized
    private fun changed(online: Boolean) {
        this.online = online
        connectivity.setOnline(online)
        sink?.setOnline(online)
    }

    private fun isOnline(manager: ConnectivityManager): Boolean {
        val capabilities = manager.getNetworkCapabilities(manager.activeNetwork) ?: return false
        return capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)
    }
}
