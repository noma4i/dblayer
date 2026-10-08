package com.noma4i.dblayer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities

class Reachability(context: Context) {
    private val manager = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
    private var callback: ConnectivityManager.NetworkCallback? = null

    fun start(sink: OnlineSink) {
        sink.setOnline(isOnline())
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) = sink.setOnline(true)
            override fun onLost(network: Network) = sink.setOnline(isOnline())
            override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) =
                sink.setOnline(capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED))
        }
        this.callback = callback
        manager.registerDefaultNetworkCallback(callback)
    }

    fun stop() {
        callback?.let { manager.unregisterNetworkCallback(it) }
        callback = null
    }

    private fun isOnline(): Boolean {
        val capabilities = manager.getNetworkCapabilities(manager.activeNetwork) ?: return false
        return capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)
    }
}
