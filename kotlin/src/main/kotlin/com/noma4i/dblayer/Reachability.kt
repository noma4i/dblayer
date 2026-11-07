package com.noma4i.dblayer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities

internal object Reachability {
    fun observe(context: Context, changed: (Boolean) -> Unit): () -> Unit {
        val service = context.applicationContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) = changed(validated(service.getNetworkCapabilities(network)))
            override fun onLost(network: Network) = changed(false)
            override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) = changed(validated(capabilities))
        }
        service.registerDefaultNetworkCallback(callback)
        changed(validated(service.getNetworkCapabilities(service.activeNetwork)))
        return { service.unregisterNetworkCallback(callback) }
    }

    private fun validated(capabilities: NetworkCapabilities?): Boolean = capabilities?.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED) == true
}
