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
        if (event.epoch != database.sessionEpoch()) return
        val refused = try {
            scheduler.start(context, event.items)
        } catch (error: RuntimeException) {
            error.message ?: error.javaClass.simpleName
        }
        if (refused != null) {
            try {
                database.backgroundRefused(event.epoch, event.items, refused)
            } catch (_: DbException.Interrupted) {
                return
            } catch (_: DbException.NoSession) {
                return
            }
        }
    }
}

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

private class RunningUploadRound(val database: Database, val epoch: ULong, val owner: UInt) {
    val networkToken = UploadJobToken()
}

private fun beginUploadRound(database: Database): RunningUploadRound? {
    val epoch = database.sessionEpoch()
    val owner = try {
        database.roundBegin(epoch)
    } catch (_: DbException.Interrupted) {
        return null
    } catch (_: DbException.NoSession) {
        return null
    } ?: return null
    return RunningUploadRound(database, epoch, owner)
}

private fun endUploadRound(context: Context, round: RunningUploadRound) {
    val groups = try {
        round.database.roundEnd(round.database.sessionEpoch(), round.owner).orEmpty()
    } catch (_: DbException.Interrupted) {
        return
    } catch (_: DbException.NoSession) {
        return
    }
    groups.forEach { UploadAcceptedStart(context, round.database).accepted(it) }
}

private fun abandonUploadRound(round: RunningUploadRound) {
    try {
        round.database.roundAbandon(round.epoch, round.owner)
    } catch (_: DbException.Interrupted) {
    } catch (_: DbException.NoSession) {
    }
}

private suspend fun runUploadRound(round: RunningUploadRound) {
    while (true) {
        val step = try {
            round.database.roundNext(round.epoch, round.owner)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: DbException.Interrupted) {
            return
        } catch (_: DbException.NoSession) {
            return
        } catch (_: Exception) {
            try {
                round.database.roundLeave(round.epoch, round.owner)
            } catch (_: DbException.Interrupted) {
                return
            } catch (_: DbException.NoSession) {
                return
            }
        }
        when (step) {
            WireRoundStep.STALE -> return
            WireRoundStep.MORE -> continue
            WireRoundStep.DONE -> return
        }
    }
}

@RequiresApi(Build.VERSION_CODES.UPSIDE_DOWN_CAKE)
internal class UploadJobService : JobService() {
    private var scope: CoroutineScope? = null
    @Volatile private var round: RunningUploadRound? = null

    override fun onStartJob(params: JobParameters): Boolean {
        setNotification(params, UploadNotifications.ID, UploadNotifications.build(this), JOB_END_NOTIFICATION_POLICY_REMOVE)
        val database = DBlayer.openedDatabase()
        val current = database?.let(::beginUploadRound)
        if (current == null) { jobFinished(params, false); return false }
        round = current
        UploadNetwork.started(params.network, current.networkToken)
        val work = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        scope = work
        work.launch {
            runUploadRound(current)
            UploadNetwork.ended(current.networkToken)
            jobFinished(params, false)
            endUploadRound(applicationContext, current)
        }
        return true
    }

    override fun onNetworkChanged(params: JobParameters) {
        val current = round ?: return
        UploadNetwork.changed(params.network, current.networkToken)
    }

    override fun onStopJob(params: JobParameters): Boolean {
        val current = round ?: return true
        UploadNetwork.ended(current.networkToken)
        scope?.cancel()
        scope = null
        round = null
        abandonUploadRound(current)
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
    @Volatile private var lastStart = 0
    @Volatile private var round: RunningUploadRound? = null

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(UploadNotifications.ID, UploadNotifications.build(this), ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        lastStart = startId
        scope.launch {
            val database = DBlayer.awaitDatabase()
            val current = beginUploadRound(database) ?: return@launch
            round = current
            runUploadRound(current)
            stopSelf(lastStart)
            endUploadRound(applicationContext, current)
        }
        return START_STICKY
    }

    override fun onDestroy() {
        scope.cancel()
        val current = round ?: return
        round = null
        abandonUploadRound(current)
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
