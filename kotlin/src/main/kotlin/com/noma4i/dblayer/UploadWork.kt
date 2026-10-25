package com.noma4i.dblayer

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.app.job.JobInfo
import android.app.job.JobParameters
import android.app.job.JobScheduler
import android.app.job.JobService
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.Network
import android.os.Build
import android.os.IBinder
import androidx.annotation.DrawableRes
import androidx.annotation.RequiresApi
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import okhttp3.Dns
import okhttp3.OkHttpClient

class UploadNotification(val title: String, val text: String, @DrawableRes val smallIcon: Int)

internal object UploadNotifications {
    private const val FILE = "com.noma4i.dblayer.uploads"
    const val CHANNEL = "com.noma4i.dblayer.uploads"
    const val ID = 0x0DB1

    fun store(context: Context, notification: UploadNotification) {
        context.getSharedPreferences(FILE, Context.MODE_PRIVATE).edit()
            .putString("title", notification.title).putString("text", notification.text).putInt("icon", notification.smallIcon).apply()
    }

    private fun stored(context: Context): UploadNotification {
        val file = context.getSharedPreferences(FILE, Context.MODE_PRIVATE)
        return UploadNotification(file.getString("title", null) ?: "Uploading", file.getString("text", null) ?: "Files are on their way", file.getInt("icon", android.R.drawable.stat_sys_upload))
    }

    fun build(context: Context): Notification {
        val shown = stored(context)
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.createNotificationChannel(NotificationChannel(CHANNEL, shown.title, NotificationManager.IMPORTANCE_LOW))
        return Notification.Builder(context, CHANNEL).setContentTitle(shown.title).setContentText(shown.text).setSmallIcon(shown.smallIcon).setOngoing(true).build()
    }
}

internal enum class UploadMode { UserInitiatedJob, ForegroundService }

internal fun uploadMode(sdk: Int): UploadMode = if (sdk >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) UploadMode.UserInitiatedJob else UploadMode.ForegroundService

internal fun interface UploadScheduler {
    fun start(context: Context, items: List<WireAcceptedUpload>): String?
}

internal object SystemUploadScheduler : UploadScheduler {
    override fun start(context: Context, items: List<WireAcceptedUpload>): String? = when (uploadMode(Build.VERSION.SDK_INT)) {
        UploadMode.UserInitiatedJob -> UploadJobService.schedule(context)
        UploadMode.ForegroundService -> UploadService.start(context)
    }
}

internal class UploadAcceptedStart(private val context: Context, private val database: Database, private val scheduler: UploadScheduler = SystemUploadScheduler) : UploadAcceptedSink {
    override fun accepted(event: WireUploadAccepted) {
        val refused = try {
            scheduler.start(context, event.items)
        } catch (error: RuntimeException) {
            error.message ?: error.javaClass.simpleName
        }
        if (refused != null) database.backgroundRefused(event.items, refused)
    }
}

internal object UploadNetwork {
    @Volatile private var active = false
    @Volatile private var network: Network? = null
    @Volatile private var owner = 0u

    @Synchronized fun started(network: Network?, owner: UInt) { this.owner = owner; this.network = network; active = true }
    @Synchronized fun changed(network: Network?) { this.network = network }
    @Synchronized fun ended(owner: UInt) { if (owner == this.owner) { active = false; network = null } }

    fun route(client: OkHttpClient): OkHttpClient {
        if (!active) return client
        val bound = network ?: throw java.io.IOException("the upload job has no network")
        return client.newBuilder().socketFactory(bound.socketFactory).dns(Dns { host -> bound.getAllByName(host).toList() }).build()
    }
}

internal suspend fun runUploadRound(database: Database, owner: UInt): Boolean {
    while (true) {
        val step = try {
            database.resumeUploads()
            database.roundNext(owner)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            database.roundLeave(owner)
        }
        when (step) {
            WireRoundStep.STALE -> return false
            WireRoundStep.MORE -> continue
            WireRoundStep.DONE -> return true
        }
    }
}

@RequiresApi(Build.VERSION_CODES.UPSIDE_DOWN_CAKE)
internal class UploadJobService : JobService() {
    private var scope: CoroutineScope? = null
    private var round: UInt? = null

    override fun onStartJob(params: JobParameters): Boolean {
        setNotification(params, UploadNotifications.ID, UploadNotifications.build(this), JOB_END_NOTIFICATION_POLICY_REMOVE)
        val database = DBlayer.openedDatabase()
        val owner = database?.roundBegin()
        if (database == null || owner == null) { jobFinished(params, false); return false }
        round = owner
        UploadNetwork.started(params.network, owner)
        val work = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        scope = work
        work.launch {
            if (!runUploadRound(database, owner)) return@launch
            UploadNetwork.ended(owner)
            jobFinished(params, false)
            val late = database.roundEnd(owner).orEmpty()
            if (late.isNotEmpty()) UploadAcceptedStart(applicationContext, database).accepted(WireUploadAccepted("", late))
        }
        return true
    }

    override fun onNetworkChanged(params: JobParameters) {
        UploadNetwork.changed(params.network)
    }

    override fun onStopJob(params: JobParameters): Boolean {
        val owner = round ?: return true
        UploadNetwork.ended(owner)
        scope?.cancel()
        scope = null
        DBlayer.openedDatabase()?.roundAbandon(owner)
        return true
    }

    companion object {
        const val JOB_ID = 0x0DB1

        fun schedule(context: Context): String? {
            val scheduler = context.getSystemService(JobScheduler::class.java)
            if (scheduler.getPendingJob(JOB_ID) != null) return null
            val job = JobInfo.Builder(JOB_ID, ComponentName(context, UploadJobService::class.java))
                .setUserInitiated(true)
                .setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY)
                .build()
            return if (scheduler.schedule(job) == JobScheduler.RESULT_SUCCESS) null else "the system refused the user-initiated job while the app was not visible"
        }
    }
}

internal class UploadService : Service() {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var lastStart = 0
    private var round: UInt? = null

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(UploadNotifications.ID, UploadNotifications.build(this), ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        lastStart = startId
        scope.launch {
            val database = DBlayer.awaitDatabase()
            val owner = database.roundBegin() ?: return@launch
            round = owner
            if (!runUploadRound(database, owner)) return@launch
            stopSelf(lastStart)
            val late = database.roundEnd(owner).orEmpty()
            if (late.isNotEmpty()) UploadAcceptedStart(applicationContext, database).accepted(WireUploadAccepted("", late))
        }
        return START_STICKY
    }

    override fun onDestroy() {
        scope.cancel()
        val owner = round ?: return
        DBlayer.openedDatabase()?.roundAbandon(owner)
    }

    companion object {
        fun start(context: Context): String? = try {
            context.startForegroundService(Intent(context, UploadService::class.java))
            null
        } catch (error: IllegalStateException) {
            error.message ?: "the system refused to start the upload service"
        }
    }
}
