package com.noma4i.dblayer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities

/** Called after the core registered an OS callback and after the upload service stopped itself. The test fixtures set it to wake their waits. */
object OsProbe {
    @Volatile var changed: (() -> Unit)? = null
}

internal object Reachability {
    /** Reports the OS value at once and on every change until the returned stop runs. */
    fun observe(context: Context, changed: (Boolean) -> Unit): () -> Unit {
        val service = context.applicationContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        fun current() = validated(service.getNetworkCapabilities(service.activeNetwork))
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onLost(network: Network) = changed(false)
            override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) = changed(validated(capabilities))
        }
        changed(current())
        service.registerDefaultNetworkCallback(callback)
        OsProbe.changed?.invoke()
        return { service.unregisterNetworkCallback(callback) }
    }

    private fun validated(capabilities: NetworkCapabilities?): Boolean = capabilities?.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED) == true
}

/** The OS observers the core starts after `open` and stops in `Closing`: each OS value goes to the sink as it comes. */
internal class SystemObservers(private val context: Context) : PlatformObserver {
    private var stops: List<() -> Unit> = emptyList()

    override fun start(sink: OnlineSink) {
        stops = listOf(
            Reachability.observe(context) { sink.setOnline(it) },
            ForegroundEvents.observe { sink.enterForeground() },
            PowerSaving.observe(context) { sink.setPowerSaving(it) },
        )
    }

    override fun stop() {
        stops.forEach { it() }
        stops = emptyList()
    }
}
