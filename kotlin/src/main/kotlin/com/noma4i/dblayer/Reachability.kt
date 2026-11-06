package com.noma4i.dblayer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import java.util.concurrent.atomic.AtomicReference

internal object Reachability {
    private class State(val manager: ConnectivityManager?, val callback: ConnectivityManager.NetworkCallback?, val online: Boolean, val sink: OnlineSink?)

    private val connectivity = Connectivity(false)
    private val state = AtomicReference(State(null, null, false, null))

    fun start(context: Context): Connectivity {
        val service = context.applicationContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) = report(isOnline(service))
            override fun onLost(network: Network) = report(isOnline(service))
            override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) = report(isOnline(service))
        }
        val first = state.getAndUpdate { if (it.manager == null) State(service, callback, it.online, it.sink) else it }.manager == null
        if (!first) return connectivity
        service.registerDefaultNetworkCallback(callback)
        report(isOnline(service))
        return connectivity
    }

    fun connect(target: OnlineSink) {
        val now = state.updateAndGet { State(it.manager, it.callback, it.online, target) }.online
        target.setOnline(now)
    }

    fun disconnect() {
        val old = state.getAndUpdate { State(null, null, it.online, null) }
        old.callback?.let { old.manager?.unregisterNetworkCallback(it) }
    }

    private fun report(now: Boolean) {
        val target = state.updateAndGet { State(it.manager, it.callback, now, it.sink) }.sink
        connectivity.setOnline(now)
        target?.setOnline(now)
    }

    private fun isOnline(manager: ConnectivityManager): Boolean {
        val capabilities = manager.getNetworkCapabilities(manager.activeNetwork) ?: return false
        return capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)
    }
}
