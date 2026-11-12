package com.noma4i.dblayer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities

internal object Reachability {
    /** Reports the default network at once and on every change until the returned stop runs. The network number grows when the `Network` object changes. */
    fun observe(context: Context, changed: (NetworkPath) -> Unit): () -> Unit {
        val service = context.applicationContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        var last: Network? = null
        var number = 0UL
        fun report(network: Network?, capabilities: NetworkCapabilities?) {
            val online = validated(capabilities)
            if (network != null && network != last) { number += 1UL; last = network }
            changed(NetworkPath(online, if (online) kind(capabilities) else NetworkKind.NONE, number))
        }
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onLost(network: Network) = report(null, null)
            override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) = report(network, capabilities)
        }
        val active = service.activeNetwork
        report(active, service.getNetworkCapabilities(active))
        service.registerDefaultNetworkCallback(callback)
        return { service.unregisterNetworkCallback(callback) }
    }

    private fun validated(capabilities: NetworkCapabilities?): Boolean = capabilities?.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED) == true

    private fun kind(capabilities: NetworkCapabilities?): NetworkKind = when {
        capabilities?.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) == true -> NetworkKind.WIFI
        capabilities?.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) == true -> NetworkKind.CELLULAR
        else -> NetworkKind.OTHER
    }
}

/** The OS observers the core starts after `open` and stops in `Closing`: each OS value goes to the sink as it comes. */
internal class SystemObservers(private val context: Context) : PlatformObserver {
    private var stops: List<() -> Unit> = emptyList()

    override fun start(sink: OnlineSink) {
        val registration = sink.registerForeground()
        stops = listOf(
            ForegroundEvents.observe { sink.enterForeground(registration) },
            PowerSaving.observe(context) { sink.setPowerSaving(it) },
        )
    }

    override fun stop() {
        stops.forEach { it() }
        stops = emptyList()
    }
}

/** The observer of `NetworkStatus`: the core starts it once per process and never stops it. */
internal class StatusObserver(private val context: Context) : PlatformObserver {
    override fun start(sink: OnlineSink) {
        Reachability.observe(context) { sink.setPath(it) }
    }

    override fun stop() {}
}
