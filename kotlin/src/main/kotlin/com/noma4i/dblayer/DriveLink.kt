package com.noma4i.dblayer

import android.app.Activity
import android.os.Handler
import android.os.Looper
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner
import com.google.android.gms.auth.api.identity.AuthorizationRequest
import com.google.android.gms.auth.api.identity.Identity
import com.google.android.gms.common.api.Scope
import com.google.android.gms.tasks.Task
import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.OkHttpClient
import java.lang.ref.WeakReference
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

private const val DRIVE_APPDATA = "https://www.googleapis.com/auth/drive.appdata"
private const val CONSENT_REQUEST = 0x0DB1

internal class AuthorizationToken(activity: Activity) : DriveToken {
    private val activity = WeakReference(activity)
    private val context = activity.applicationContext

    override suspend fun accessToken(): String {
        val request = AuthorizationRequest.builder().setRequestedScopes(listOf(Scope(DRIVE_APPDATA))).build()
        val result = try {
            Identity.getAuthorizationClient(context).authorize(request).await()
        } catch (error: Exception) {
            throw CloudException.Unavailable(error.message ?: error.javaClass.simpleName)
        }
        val intent = result.pendingIntent
        if (result.hasResolution() && intent != null) {
            Handler(Looper.getMainLooper()).post {
                activity.get()?.startIntentSenderForResult(intent.intentSender, CONSENT_REQUEST, null, 0, 0, 0)
            }
            throw CloudException.NeedsAuthorization("Drive access needs the user consent")
        }
        return result.accessToken ?: throw CloudException.NeedsAuthorization("Drive returned no access token")
    }
}

private suspend fun <T> Task<T>.await(): T = suspendCancellableCoroutine { continuation ->
    addOnSuccessListener { continuation.resume(it) }
    addOnFailureListener { continuation.resumeWithException(it) }
}

class DriveLink internal constructor(val link: CloudLink) {
    private val observer = object : DefaultLifecycleObserver {
        override fun onStart(owner: LifecycleOwner) = link.remoteChanged()
    }

    init {
        Handler(Looper.getMainLooper()).post { ProcessLifecycleOwner.get().lifecycle.addObserver(observer) }
    }

    fun stop() {
        Handler(Looper.getMainLooper()).post { ProcessLifecycleOwner.get().lifecycle.removeObserver(observer) }
        link.stop()
    }
}

fun Database.connectDrive(activity: Activity): DriveLink {
    val token = AuthorizationToken(activity)
    return DriveLink(connectCloud(DriveCloud(DriveApi(OkHttpClient(), token), token)))
}
