package com.noma4i.dblayer

import java.io.File
import java.io.IOException
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl

sealed interface DriveAuthorization {
    data class Available(val key: String, val accessToken: String) : DriveAuthorization
    data object SignedOut : DriveAuthorization
    data object Unavailable : DriveAuthorization
}

class DriveSync internal constructor(
    private val authorization: StateFlow<DriveAuthorization>,
    private val directory: File,
    private val replica: String,
    private val drive: DriveChanges = DriveChanges(),
    private val relay: HttpUrl? = null,
) : CloudPort {
    companion object {
        fun configuration(application: CloudApplication, authorization: StateFlow<DriveAuthorization>, directory: File, replica: String, relay: String? = null): CloudConfig {
            require(replica.isNotEmpty())
            val origin = relay?.toHttpUrl()?.also { require(it.isHttps && it.username.isEmpty() && it.password.isEmpty()) }
                ?.newBuilder()?.encodedPath("/")?.query(null)?.fragment(null)?.build()
            return CloudConfig(CloudBackend.DRIVE, application, DriveSync(authorization, directory, replica, relay = origin))
        }
    }

    private var observer: Job? = null
    private val active = AtomicReference<Generation?>(null)

    override suspend fun observe(signals: CloudSignals) {
        observer?.cancelAndJoin()
        observer = CoroutineScope(SupervisorJob() + Dispatchers.IO).launch {
            authorization.collect { state ->
                signals.account(when (state) {
                    is DriveAuthorization.Available -> CloudAccount.Available(state.key)
                    DriveAuthorization.SignedOut -> CloudAccount.SignedOut
                    DriveAuthorization.Unavailable -> CloudAccount.Unavailable
                })
            }
        }
    }

    override suspend fun unobserve() {
        observer?.cancelAndJoin()
        observer = null
    }

    override suspend fun start(scope: CloudWireScope, progress: ByteArray?, signals: CloudSignals) {
        val auth = authorization.value as? DriveAuthorization.Available ?: throw DbException.NoSession("drive account unavailable")
        val fingerprint = MessageDigest.getInstance("SHA-256").digest(auth.key.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it.toInt() and 0xff) }
        if (scope.providerFingerprint != fingerprint) throw DbException.NoSession("drive account changed")
        val generation = Generation(scope, auth.key, signals)
        if (!active.compareAndSet(null, generation)) throw DbException.InFlight("cloud generation active")
        try {
            generation.run { fetch(progress) }
            generation.ready.complete(Unit)
            generation.observeForeground()
        } catch (error: Throwable) {
            generation.ready.completeExceptionally(error)
            stop(scope.generation)
            throw error
        }
    }

    override suspend fun stop(generation: ULong) {
        val current = active.get()?.takeIf { it.scope.generation == generation } ?: return
        current.job.cancelAndJoin()
        current.cleanFiles()
        active.compareAndSet(current, null)
    }

    override suspend fun send(scope: CloudWireScope, records: List<CloudOutbound>): List<CloudSendResult> {
        val current = active.get()?.takeIf { it.scope == scope } ?: throw DbException.NoSession("cloud generation ended")
        return current.run { ready.await(); sendRecords(records) }
    }

    private inner class Generation(val scope: CloudWireScope, private val key: String, private val signals: CloudSignals) {
        val job = SupervisorJob()
        private val tempDirectory = File(directory, "dblayer-drive-${UUID.randomUUID()}")
        private lateinit var cursor: String
        val ready = CompletableDeferred<Unit>()

        suspend fun <T> run(block: suspend Generation.() -> T): T = coroutineScope {
            val task = async(Dispatchers.IO + job) { block() }
            try { task.await() } finally { withContext(NonCancellable) { task.cancelAndJoin() } }
        }

        private fun token(): String {
            val current = authorization.value as? DriveAuthorization.Available
            if (current == null || current.key != key) throw DbException.NoSession("drive account changed")
            return current.accessToken
        }

        private fun temporary(): File {
            tempDirectory.mkdirs()
            return File.createTempFile("drive-", ".tmp", tempDirectory)
        }

        private suspend fun receive(fileId: String) {
            val file = temporary()
            try {
                drive.download(token(), fileId, file)
                val batch = mutableListOf<CloudInbound>()
                DriveJournal.readLatest(file) { entry ->
                    batch.add(CloudInbound(entry.key, entry.payload, null))
                    if (batch.size == 16) {
                        signals.fetched(CloudFetched(scope, batch.toList(), null, false))
                        batch.clear()
                    }
                }
                if (batch.isNotEmpty()) signals.fetched(CloudFetched(scope, batch.toList(), null, false))
            } finally {
                file.delete()
            }
        }

        suspend fun fetch(progress: ByteArray?) {
            var cursor = progress?.let { Charsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(it)).toString() }?.also { require(it.isNotEmpty()) }
            if (cursor == null) {
                cursor = drive.startPageToken(token())
                var page: String? = null
                do {
                    val result = drive.files(token(), scope.domain, page)
                    result.files.forEach { receive(it.id) }
                    page = result.nextPageToken
                } while (page != null)
            }
            readChanges(cursor, initial = true)
        }

        private suspend fun readChanges(start: String, initial: Boolean = false) {
            var next = start
            do {
                val result = drive.page(token(), next)
                result.changes.filter { !it.removed && it.appProperties["domain"] == scope.domain }
                    .map { it.fileId }.distinct().forEach { receive(it) }
                next = result.nextPageToken ?: result.newStartPageToken!!
                signals.fetched(CloudFetched(scope, emptyList(), next.toByteArray(Charsets.UTF_8), initial && result.nextPageToken == null))
                cursor = next
            } while (result.nextPageToken != null)
        }

        fun cleanFiles() {
            tempDirectory.listFiles()?.forEach { it.delete() }
            tempDirectory.delete()
        }

        fun observeForeground() {
            val origin = relay ?: return
            CoroutineScope(job + Dispatchers.IO).launch {
                callbackFlow {
                    val stop = ForegroundEvents.observe(background = { trySend(false) }) { trySend(true) }
                    awaitClose { stop() }
                }.collectLatest { foreground ->
                    if (foreground) watchForeground(origin)
                    else signals.relay(scope, CloudRelayEvent.Suspended)
                }
            }
        }

        private suspend fun watchForeground(origin: HttpUrl) {
            val watch = DriveWatch(origin)
            var channel: DriveChannel? = null
            try {
                watch.events().collect { event ->
                    when (event) {
                        DriveWatchEvent.READY -> {
                            if (channel != null) throw IOException("duplicate relay ready")
                            val registered = drive.watch(token(), cursor, origin.newBuilder().encodedPath("/drive/notifications").build().toString(), watch.secret)
                            channel = registered
                            val remaining = (registered.expiration.toLong() - System.currentTimeMillis()).coerceAtLeast(0).toULong()
                            signals.relay(scope, CloudRelayEvent.Lease(remaining))
                            readChanges(cursor)
                        }
                        DriveWatchEvent.CHANGED -> readChanges(cursor)
                    }
                }
            } catch (error: CancellationException) {
                throw error
            } catch (_: Exception) {
            } finally {
                channel?.let { registered ->
                    withContext(NonCancellable) {
                        runCatching { withTimeoutOrNull(10000) { drive.stop(token(), registered) } }
                    }
                }
            }
            signals.relay(scope, CloudRelayEvent.Ended)
        }

        suspend fun sendRecords(records: List<CloudOutbound>): List<CloudSendResult> {
            var page: String? = null
            var fileId: String? = null
            do {
                val result = drive.files(token(), scope.domain, page)
                fileId = result.files.firstOrNull { it.replica == replica }?.id
                page = result.nextPageToken
            } while (fileId == null && page != null)
            val create = fileId == null
            val id = fileId ?: drive.reserveId(token())
            val current = if (create) null else temporary().also { drive.download(token(), id, it) }
            val candidate = temporary()
            try {
                DriveJournal.prepare(current, candidate, records.map { DriveJournalEntry(it.key, it.payload) })
                try {
                    drive.upload(token(), scope.domain, replica, id, create, candidate) { token() }
                } catch (error: DriveHttpException) {
                    if (!create || error.status != 409) throw error
                    drive.upload(token(), scope.domain, replica, id, false, candidate) { token() }
                }
                return records.map { CloudSendResult(it.key, it.version, CloudSendStatus.Saved(null)) }
            } finally {
                current?.delete()
                candidate.delete()
            }
        }
    }
}
