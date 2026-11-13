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
        NetworkStatus(context)
        eventually("the core started the network observer") { networkRegistrations.get() == 1 }
        val encrypted = EncryptedConfig("https://example.com", "", "", EncryptedEndpoints("/graphql", "/cable", "/logout", "/bootstrap", emptyMap()), "test-v1", "hello", "TestChannel")
        val http = object : ScriptedHttp {
            override suspend fun post(path: String, body: String, purpose: NetworkPurpose): EncryptedReply = throw kotlinx.coroutines.CancellationException()
        }
        val config = async(start = CoroutineStart.UNDISPATCHED) { runCatching { DBlayer.serverConfig(context, encrypted, AppPlatform.ANDROID, "1.2", http = http) } }
        config.cancel()
        assertTrue(runCatching { DBlayer.open(context, modelsJson = "invalid", uploadNotification = UploadNotification("Uploading", "Files are on their way", 1)) }.exceptionOrNull() is DbException.Invalid)
        assertEquals(1, networkRegistrations.get())
    }

    /** net_status_observer_starts_once: the second and third `NetworkStatus(context)` start no new observer of the platform. */
    @Test
    fun net_status_kotlinFurtherStatusesStartNoSecondObserver() = bounded {
        val context = org.robolectric.RuntimeEnvironment.getApplication()
        NetworkStatus(context)
        NetworkStatus(context)
        NetworkStatus(context)
        eventually("the core started the network observer") { networkRegistrations.get() == 1 }
        assertEquals(1, networkRegistrations.get())
    }
}
