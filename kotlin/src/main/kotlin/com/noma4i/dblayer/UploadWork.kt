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

internal fun interface UploadScheduler {
    fun start(context: Context, requestId: ULong, epoch: ULong): String?
}

internal object SystemUploadScheduler : UploadScheduler {
    override fun start(context: Context, requestId: ULong, epoch: ULong): String? = when (uploadMode(Build.VERSION.SDK_INT)) {
        UploadMode.UserInitiatedJob -> UploadJobService.schedule(context, requestId, epoch)
        UploadMode.ForegroundService -> UploadService.start(context, requestId, epoch)
    }
}

/** The jobs of the rounds of the core: the core asks for a job, the system runs it, the core ends it. */
internal class UploadRounds(private val context: Context, private val database: Database, private val scheduler: UploadScheduler = SystemUploadScheduler) : RoundJobsSink {
    override fun scheduleJob(requestId: ULong, epoch: ULong) {
        val refused = try {
            scheduler.start(context, requestId, epoch)
        } catch (error: RuntimeException) {
            error.message ?: error.javaClass.simpleName
        }
        if (refused != null) database.roundRefused(requestId, epoch, refused)
    }

    override fun endJob(roundId: UInt) {
        when (uploadMode(Build.VERSION.SDK_INT)) {
            UploadMode.UserInitiatedJob -> context.getSystemService(JobScheduler::class.java).cancel(UploadJobService.JOB_ID)
            UploadMode.ForegroundService -> context.stopService(Intent(context, UploadService::class.java))
        }
    }
}

private const val REQUEST = "request"
private const val EPOCH = "epoch"

/** The round id of a request: the core numbers requests and rounds apart, the low 32 bits of the request name its round. */
internal fun roundIdOf(requestId: ULong): UInt = requestId.toUInt()

internal class UploadJobToken

internal object UploadNetwork {
    private class Route(val owner: UploadJobToken, val network: Network?)
    @Volatile private var current: Route? = null

    @Synchronized fun started(network: Network?, owner: UploadJobToken) { current = Route(owner, network) }
    @Synchronized fun changed(network: Network?, owner: UploadJobToken) { if (owner === current?.owner) current = Route(owner, network) }
    @Synchronized fun ended(owner: UploadJobToken) { if (owner === current?.owner) current = null }

    fun route(client: OkHttpClient): OkHttpClient {
        val route = current ?: return client
        val bound = route.network ?: throw java.io.IOException("the upload job has no network")
        return client.newBuilder().socketFactory(bound.socketFactory).dns(Dns { host -> bound.getAllByName(host).toList() }).build()
    }
}

@RequiresApi(Build.VERSION_CODES.UPSIDE_DOWN_CAKE)
internal class UploadJobService : JobService() {
    @Volatile private var running: Triple<UInt, ULong, UploadJobToken>? = null

    override fun onStartJob(params: JobParameters): Boolean {
        setNotification(params, UploadNotifications.ID, UploadNotifications.build(this), JOB_END_NOTIFICATION_POLICY_REMOVE)
        val database = DBlayer.openedDatabase()
        val requestId = params.extras.getLong(REQUEST).toULong()
        val epoch = params.extras.getLong(EPOCH).toULong()
        if (database == null) { jobFinished(params, false); return false }
        val roundId = roundIdOf(requestId)
        val token = UploadJobToken()
        running = Triple(roundId, epoch, token)
        UploadNetwork.started(params.network, token)
        database.roundStarted(requestId, roundId, epoch)
        return true
    }

    override fun onNetworkChanged(params: JobParameters) {
        val (_, _, token) = running ?: return
        UploadNetwork.changed(params.network, token)
    }

    override fun onStopJob(params: JobParameters): Boolean {
        val (roundId, epoch, token) = running ?: return true
        UploadNetwork.ended(token)
        running = null
        DBlayer.openedDatabase()?.roundEnded(roundId, epoch)
        return true
    }

    companion object {
        const val JOB_ID = 0x0DB1

        fun schedule(context: Context, requestId: ULong, epoch: ULong): String? {
            val scheduler = context.getSystemService(JobScheduler::class.java)
            if (scheduler.getPendingJob(JOB_ID) != null) return null
            val extras = android.os.PersistableBundle().apply { putLong(REQUEST, requestId.toLong()); putLong(EPOCH, epoch.toLong()) }
            val job = JobInfo.Builder(JOB_ID, ComponentName(context, UploadJobService::class.java))
                .setUserInitiated(true)
                .setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY)
                .setExtras(extras)
                .build()
            return if (scheduler.schedule(job) == JobScheduler.RESULT_SUCCESS) null else "the system refused the user-initiated job while the app was not visible"
        }
    }
}

internal class UploadService : Service() {
    @Volatile private var running: Pair<UInt, ULong>? = null

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(UploadNotifications.ID, UploadNotifications.build(this), ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        val requestId = intent?.getLongExtra(REQUEST, 0)?.toULong() ?: return START_STICKY
        val epoch = intent.getLongExtra(EPOCH, 0).toULong()
        val roundId = roundIdOf(requestId)
        running = roundId to epoch
        DBlayer.openedDatabase()?.roundStarted(requestId, roundId, epoch)
        return START_STICKY
    }

    override fun onDestroy() {
        val (roundId, epoch) = running ?: return
        running = null
        DBlayer.openedDatabase()?.roundEnded(roundId, epoch)
    }

    companion object {
        fun start(context: Context, requestId: ULong, epoch: ULong): String? = try {
            context.startForegroundService(Intent(context, UploadService::class.java).putExtra(REQUEST, requestId.toLong()).putExtra(EPOCH, epoch.toLong()))
            null
        } catch (error: IllegalStateException) {
            error.message ?: "the system refused to start the upload service"
        }
    }
}
