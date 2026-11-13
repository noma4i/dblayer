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

    fun build(context: Context, fraction: Double? = null): Notification {
        val shown = stored(context)
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.createNotificationChannel(NotificationChannel(CHANNEL, shown.title, NotificationManager.IMPORTANCE_LOW))
        val builder = Notification.Builder(context, CHANNEL).setContentTitle(shown.title).setContentText(shown.text).setSmallIcon(shown.smallIcon).setOngoing(true)
        if (fraction != null) builder.setProgress(100, (fraction * 100).toInt(), false)
        return builder.build()
    }
}

internal enum class UploadMode { UserInitiatedJob, ForegroundService }

internal fun uploadMode(sdk: Int): UploadMode = if (sdk >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) UploadMode.UserInitiatedJob else UploadMode.ForegroundService

internal const val DATA_SYNC_PERMISSION = "android.permission.FOREGROUND_SERVICE_DATA_SYNC"

/** Below API 34 the round runs in a foreground service of type `dataSync`: the manifest of the app has to request its permission, and the core refuses `open` when it does not. `null` where the platform needs no declaration. */
internal fun dataSyncDeclared(sdk: Int, requested: () -> Collection<String>): Boolean? =
    if (uploadMode(sdk) == UploadMode.ForegroundService) DATA_SYNC_PERMISSION in requested() else null

internal class Refusal(val kind: RoundRefusal, val detail: String)

internal fun interface UploadScheduler {
    fun start(context: Context, job: RoundJob): Refusal?
}

internal object SystemUploadScheduler : UploadScheduler {
    override fun start(context: Context, job: RoundJob): Refusal? = when (uploadMode(Build.VERSION.SDK_INT)) {
        UploadMode.UserInitiatedJob -> UploadJobService.schedule(context, job)
        UploadMode.ForegroundService -> UploadService.start(context, job)
    }
}

/** The jobs of the rounds of the core: the core asks for a job, the system runs it, the core ends it. */
internal class UploadRounds(private val context: Context, private val database: Database, private val scheduler: UploadScheduler = SystemUploadScheduler) : RoundJobsSink {
    override fun scheduleJob(job: RoundJob) {
        UploadNetwork.requested(job)
        val refusal = try {
            scheduler.start(context, job)
        } catch (error: RuntimeException) {
            Refusal(RoundRefusal.SCHEDULER_FAILED, error.message ?: error.javaClass.simpleName)
        }
        if (refusal != null) database.roundRefused(job.requestId, job.epoch, refusal.kind, refusal.detail)
    }

    override fun endJob(roundId: UInt) {
        UploadNetwork.ended(roundId)
        val scheduler = context.getSystemService(JobScheduler::class.java)
        if (scheduler.getPendingJob(UploadJobService.JOB_ID)?.extras?.let(::roundJobOf)?.roundId == roundId) scheduler.cancel(UploadJobService.JOB_ID)
        UploadService.stop(roundId)
    }

    override fun roundProgress(roundId: UInt, fraction: Double) {
        val notification = UploadNotifications.build(context, fraction)
        when (uploadMode(Build.VERSION.SDK_INT)) {
            UploadMode.UserInitiatedJob -> UploadJobService.progress(roundId, notification)
            UploadMode.ForegroundService -> (context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager).notify(UploadNotifications.ID, notification)
        }
    }
}

private const val REQUEST = "request"
private const val ROUND = "round"
private const val EPOCH = "epoch"

internal fun roundJobOf(extras: android.os.PersistableBundle) = RoundJob(extras.getLong(REQUEST).toULong(), extras.getLong(ROUND).toUInt(), extras.getLong(EPOCH).toULong())

/** The networks of the started jobs by the full identity of the round. The network of the last requested round is the one the uploads use: a late callback of another round holds its own entry and never replaces it. */
internal object UploadNetwork {
    private data class Held(val networks: Map<RoundJob, Network?> = emptyMap(), val requested: RoundJob? = null, val active: RoundJob? = null)
    private val held = java.util.concurrent.atomic.AtomicReference(Held())

    fun requested(job: RoundJob) { held.updateAndGet { it.copy(requested = job) } }
    fun started(job: RoundJob, network: Network?) { held.updateAndGet { it.copy(networks = it.networks + (job to network), active = if (job == it.requested) job else it.active) } }
    fun changed(job: RoundJob, network: Network?) { held.updateAndGet { if (job in it.networks) it.copy(networks = it.networks + (job to network)) else it } }
    fun ended(roundId: UInt) { held.updateAndGet { it.copy(networks = it.networks.filterKeys { job -> job.roundId != roundId }, active = it.active?.takeIf { job -> job.roundId != roundId }) } }

    fun route(client: OkHttpClient): OkHttpClient {
        val now = held.get()
        val job = now.active ?: return client
        val bound = now.networks[job] ?: throw java.io.IOException("the upload job has no network")
        return client.newBuilder().socketFactory(bound.socketFactory).dns(Dns { host -> bound.getAllByName(host).toList() }).build()
    }
}

@RequiresApi(Build.VERSION_CODES.UPSIDE_DOWN_CAKE)
internal class UploadJobService : JobService() {
    override fun onStartJob(params: JobParameters): Boolean {
        running.set(this to params)
        setNotification(params, UploadNotifications.ID, UploadNotifications.build(this), JOB_END_NOTIFICATION_POLICY_REMOVE)
        val database = DBlayer.openedDatabase()
        if (database == null) { jobFinished(params, false); return false }
        val job = roundJobOf(params.extras)
        UploadNetwork.started(job, params.network)
        database.roundStarted(job.requestId, job.roundId, job.epoch)
        return true
    }

    override fun onNetworkChanged(params: JobParameters) {
        UploadNetwork.changed(roundJobOf(params.extras), params.network)
    }

    override fun onStopJob(params: JobParameters): Boolean {
        val job = roundJobOf(params.extras)
        running.compareAndSet(running.get()?.takeIf { it.second === params }, null)
        UploadNetwork.ended(job.roundId)
        DBlayer.openedDatabase()?.roundEnded(job.roundId, job.epoch)
        return false
    }

    companion object {
        const val JOB_ID = 0x0DB1

        private val running = java.util.concurrent.atomic.AtomicReference<Pair<UploadJobService, JobParameters>?>(null)

        fun progress(roundId: UInt, notification: Notification) {
            val (service, params) = running.get()?.takeIf { roundJobOf(it.second.extras).roundId == roundId } ?: return
            service.setNotification(params, UploadNotifications.ID, notification, JOB_END_NOTIFICATION_POLICY_REMOVE)
        }

        fun schedule(context: Context, job: RoundJob): Refusal? {
            val scheduler = context.getSystemService(JobScheduler::class.java)
            val extras = android.os.PersistableBundle().apply { putLong(REQUEST, job.requestId.toLong()); putLong(ROUND, job.roundId.toLong()); putLong(EPOCH, job.epoch.toLong()) }
            val info = JobInfo.Builder(JOB_ID, ComponentName(context, UploadJobService::class.java))
                .setUserInitiated(true)
                .setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY)
                .setExtras(extras)
                .build()
            return if (scheduler.schedule(info) == JobScheduler.RESULT_SUCCESS) null else Refusal(RoundRefusal.JOB_REFUSED, "")
        }
    }
}

internal class UploadService : Service() {
    private data class Seen(val rounds: List<RoundJob> = emptyList(), val unreported: List<RoundJob> = emptyList(), val ending: Set<UInt> = emptySet())
    private val seen = java.util.concurrent.atomic.AtomicReference(Seen())

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(UploadNotifications.ID, UploadNotifications.build(this), ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        live = this
        val requested = intent?.takeIf { it.hasExtra(REQUEST) }
        if (requested == null) {
            stopSelf()
            return START_NOT_STICKY
        }
        val job = RoundJob(requested.getLongExtra(REQUEST, 0).toULong(), requested.getLongExtra(ROUND, 0).toUInt(), requested.getLongExtra(EPOCH, 0).toULong())
        val database = DBlayer.openedDatabase()
        seen.updateAndGet { it.copy(rounds = it.rounds + job, unreported = if (database == null) it.unreported + job else it.unreported) }
        database?.roundStarted(job.requestId, job.roundId, job.epoch)
        return START_STICKY
    }

    override fun onDestroy() {
        if (live === this) live = null
        val database = DBlayer.openedDatabase() ?: return
        val last = seen.getAndSet(Seen())
        last.rounds.filter { it !in last.unreported }.forEach { database.roundEnded(it.roundId, it.epoch) }
    }

    private fun reportWaiting(database: Database) {
        seen.getAndUpdate { it.copy(unreported = emptyList()) }.unreported.forEach { database.roundStarted(it.requestId, it.roundId, it.epoch) }
    }

    private fun release(roundId: UInt) {
        val now = seen.updateAndGet { it.copy(ending = it.ending + roundId) }
        if (now.rounds.all { it.roundId in now.ending }) stopSelf()
    }

    companion object {
        @Volatile private var live: UploadService? = null

        fun opened(database: Database) { live?.reportWaiting(database) }

        fun stop(roundId: UInt) { live?.release(roundId) }

        fun start(context: Context, job: RoundJob): Refusal? = try {
            context.startForegroundService(Intent(context, UploadService::class.java).putExtra(REQUEST, job.requestId.toLong()).putExtra(ROUND, job.roundId.toLong()).putExtra(EPOCH, job.epoch.toLong()))
            null
        } catch (error: IllegalStateException) {
            Refusal(RoundRefusal.START_NOT_ALLOWED, error.message ?: "")
        }
    }
}
