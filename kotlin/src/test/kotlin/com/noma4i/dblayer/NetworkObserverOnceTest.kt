package com.noma4i.dblayer

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@org.junit.runner.RunWith(org.robolectric.RobolectricTestRunner::class)
@org.robolectric.annotation.Config(sdk = [35], shadows = [PulsingConnectivityManager::class])
class NetworkObserverOnceTest {
    /** net_observer_starts_once_for_database_and_config: `NetworkStatus(context)`, `serverConfig` and `open` in one process leave the one network observer the first status started. */
    @Test
    fun net_observer_starts_once_for_database_and_config() = bounded {
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        val manager = context.getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as android.net.ConnectivityManager
        val shadow = org.robolectric.Shadows.shadowOf(manager)
        NetworkStatus(context)
        val registered = shadow.networkCallbacks.size
        val encrypted = EncryptedConfig("https://example.com", "", "", EncryptedEndpoints("/graphql", "/cable", "/logout", "/bootstrap", emptyMap()), "test-v1", "hello", "TestChannel")
        val http = object : ScriptedHttp {
            override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply = throw kotlinx.coroutines.CancellationException()
        }
        val config = async(start = CoroutineStart.UNDISPATCHED) { runCatching { DBlayer.serverConfig(context, encrypted, AppPlatform.ANDROID, "1.2", http = http) } }
        config.cancel()
        assertTrue(runCatching { DBlayer.open(context, modelsJson = "invalid", uploadNotification = UploadNotification("Uploading", "Files are on their way", 1)) }.exceptionOrNull() is DbException.Invalid)
        assertEquals(registered, shadow.networkCallbacks.size)
    }

    /** net_status_observer_starts_once: the second and third `NetworkStatus(context)` start no new observer of the platform. */
    @Test
    fun net_status_kotlinFurtherStatusesStartNoSecondObserver() = bounded {
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        val manager = context.getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as android.net.ConnectivityManager
        val shadow = org.robolectric.Shadows.shadowOf(manager)
        NetworkStatus(context)
        val registered = shadow.networkCallbacks.size
        NetworkStatus(context)
        NetworkStatus(context)
        assertEquals(registered, shadow.networkCallbacks.size)
    }
}
