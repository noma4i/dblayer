package com.noma4i.dblayer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities

internal object Reachability {
    private val connectivity = Connectivity(false)
    private val lock = Any()
    private var manager: ConnectivityManager? = null
    private var online = false
    private var sink: OnlineSink? = null

    fun start(context: Context): Connectivity {
        val made = synchronized(lock) {
            if (manager != null) return connectivity
            val service = context.applicationContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
            manager = service
            service.registerDefaultNetworkCallback(object : ConnectivityManager.NetworkCallback() {
                override fun onAvailable(network: Network) = report(isOnline(service))
                override fun onLost(network: Network) = report(isOnline(service))
                override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) = report(isOnline(service))
            })
            service
        }
        report(isOnline(made))
        return connectivity
    }

    fun connect(target: OnlineSink) {
        val now = synchronized(lock) { sink = target; online }
        target.setOnline(now)
    }

    fun disconnect() = synchronized(lock) { sink = null }

    private fun report(now: Boolean) {
        val target = synchronized(lock) { online = now; sink }
        connectivity.setOnline(now)
        target?.setOnline(now)
    }

    private fun isOnline(manager: ConnectivityManager): Boolean {
        val capabilities = manager.getNetworkCapabilities(manager.activeNetwork) ?: return false
        return capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)
    }
}
