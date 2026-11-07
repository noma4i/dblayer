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
        val title = file.getString("title", null)
        val text = file.getString("text", null)
        check(title != null && text != null && file.contains("icon")) { "the upload notification is not declared: pass uploadNotification to DBlayer.open" }
        return UploadNotification(title, text, file.getInt("icon", 0))
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

internal const val DATA_SYNC_PERMISSION = "android.permission.FOREGROUND_SERVICE_DATA_SYNC"

/** Below API 34 the round runs in a foreground service of type `dataSync`: the manifest of the app has to request its permission. */
internal fun checkUploadPermission(sdk: Int, requested: () -> Collection<String>) {
    if (uploadMode(sdk) != UploadMode.ForegroundService || DATA_SYNC_PERMISSION in requested()) return
    throw DbException.Config(ConfigCode.INVALID_REQUEST, "the manifest of the app lacks $DATA_SYNC_PERMISSION")
}

internal fun interface UploadScheduler {
    fun start(context: Context, job: RoundJob): String?
}

internal object SystemUploadScheduler : UploadScheduler {
    override fun start(context: Context, job: RoundJob): String? = when (uploadMode(Build.VERSION.SDK_INT)) {
        UploadMode.UserInitiatedJob -> UploadJobService.schedule(context, job)
        UploadMode.ForegroundService -> UploadService.start(context, job)
    }
}

/** The jobs of the rounds of the core: the core asks for a job, the system runs it, the core ends it. */
internal class UploadRounds(private val context: Context, private val database: Database, private val scheduler: UploadScheduler = SystemUploadScheduler) : RoundJobsSink {
    override fun scheduleJob(job: RoundJob) {
        val refused = try {
            scheduler.start(context, job)
        } catch (error: RuntimeException) {
            error.message ?: error.javaClass.simpleName
        }
        if (refused != null) database.roundRefused(job.requestId, job.epoch, refused)
    }

    override fun endJob(roundId: UInt) {
        context.getSystemService(JobScheduler::class.java).cancel(UploadJobService.JOB_ID)
        UploadService.stop()
    }
}

private const val REQUEST = "request"
private const val ROUND = "round"
private const val EPOCH = "epoch"

internal fun roundJobOf(extras: android.os.PersistableBundle) = RoundJob(extras.getLong(REQUEST).toULong(), extras.getLong(ROUND).toUInt(), extras.getLong(EPOCH).toULong())

internal object UploadNetwork {
    private class Route(val roundId: UInt, val network: Network?)
    private val current = java.util.concurrent.atomic.AtomicReference<Route?>(null)

    fun started(roundId: UInt, network: Network?) { current.set(Route(roundId, network)) }
    fun changed(roundId: UInt, network: Network?) { current.updateAndGet { if (it?.roundId == roundId) Route(roundId, network) else it } }
    fun ended(roundId: UInt) { current.updateAndGet { if (it?.roundId == roundId) null else it } }

    fun route(client: OkHttpClient): OkHttpClient {
        val route = current.get() ?: return client
        val bound = route.network ?: throw java.io.IOException("the upload job has no network")
        return client.newBuilder().socketFactory(bound.socketFactory).dns(Dns { host -> bound.getAllByName(host).toList() }).build()
    }
}

@RequiresApi(Build.VERSION_CODES.UPSIDE_DOWN_CAKE)
internal class UploadJobService : JobService() {
    override fun onStartJob(params: JobParameters): Boolean {
        setNotification(params, UploadNotifications.ID, UploadNotifications.build(this), JOB_END_NOTIFICATION_POLICY_REMOVE)
        val database = DBlayer.openedDatabase()
        if (database == null) { jobFinished(params, false); return false }
        val job = roundJobOf(params.extras)
        UploadNetwork.started(job.roundId, params.network)
        database.roundStarted(job.requestId, job.roundId, job.epoch)
        return true
    }

    override fun onNetworkChanged(params: JobParameters) {
        UploadNetwork.changed(roundJobOf(params.extras).roundId, params.network)
    }

    override fun onStopJob(params: JobParameters): Boolean {
        val job = roundJobOf(params.extras)
        UploadNetwork.ended(job.roundId)
        DBlayer.openedDatabase()?.roundEnded(job.roundId, job.epoch)
        return true
    }

    companion object {
        const val JOB_ID = 0x0DB1

        fun schedule(context: Context, job: RoundJob): String? {
            val scheduler = context.getSystemService(JobScheduler::class.java)
            val extras = android.os.PersistableBundle().apply { putLong(REQUEST, job.requestId.toLong()); putLong(ROUND, job.roundId.toLong()); putLong(EPOCH, job.epoch.toLong()) }
            val info = JobInfo.Builder(JOB_ID, ComponentName(context, UploadJobService::class.java))
                .setUserInitiated(true)
                .setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY)
                .setExtras(extras)
                .build()
            return if (scheduler.schedule(info) == JobScheduler.RESULT_SUCCESS) null else "the system refused the user-initiated job while the app was not visible"
        }
    }
}

internal class UploadService : Service() {
    @Volatile private var round: RoundJob? = null
    @Volatile private var announced = false

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(UploadNotifications.ID, UploadNotifications.build(this), ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        live = this
        val requested = intent?.takeIf { it.hasExtra(REQUEST) }
        if (requested == null) {
            stopSelf()
            return START_NOT_STICKY
        }
        round = RoundJob(requested.getLongExtra(REQUEST, 0).toULong(), requested.getLongExtra(ROUND, 0).toUInt(), requested.getLongExtra(EPOCH, 0).toULong())
        DBlayer.openedDatabase()?.let(::report)
        return START_STICKY
    }

    override fun onDestroy() {
        if (live === this) live = null
        val job = round?.takeIf { announced } ?: return
        round = null
        DBlayer.openedDatabase()?.roundEnded(job.roundId, job.epoch)
    }

    private fun report(database: Database) {
        val job = round ?: return
        if (announced) return
        announced = true
        database.roundStarted(job.requestId, job.roundId, job.epoch)
    }

    companion object {
        @Volatile private var live: UploadService? = null

        fun opened(database: Database) { live?.report(database) }

        fun stop() { live?.stopSelf() }

        fun start(context: Context, job: RoundJob): String? = try {
            context.startForegroundService(Intent(context, UploadService::class.java).putExtra(REQUEST, job.requestId.toLong()).putExtra(ROUND, job.roundId.toLong()).putExtra(EPOCH, job.epoch.toLong()))
            null
        } catch (error: IllegalStateException) {
            error.message ?: "the system refused to start the upload service"
        }
    }
}
