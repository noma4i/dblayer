package com.noma4i.dblayer

import kotlinx.coroutines.CompletableDeferred

class ManualTimers {
    private class Sleeper(val due: Double, val seconds: Double, val done: CompletableDeferred<Unit>)

    private val lock = Any()
    private var elapsed = 0.0
    private var skew = 0.0
    private val sleepers = mutableListOf<Sleeper>()
    private val recycled = java.util.concurrent.CopyOnWriteArrayList<CableRecycle>()
    var fired = 0
        private set

    val recycles: List<CableRecycle> get() = recycled.toList()

    val timers = CableTimers(
        now = { synchronized(lock) { elapsed + skew } },
        sleep = { seconds ->
            val sleeper = Sleeper(synchronized(lock) { elapsed } + seconds, seconds, CompletableDeferred())
            synchronized(lock) { sleepers.add(sleeper) }
            try {
                sleeper.done.await()
            } finally {
                synchronized(lock) { sleepers.remove(sleeper) }
            }
        },
        jitter = { 0.0 },
        recycled = { recycled.add(it) },
    )

    fun advance(seconds: Double) {
        val due = synchronized(lock) {
            elapsed += seconds
            sleepers.filter { it.due <= elapsed }.also { sleepers.removeAll(it); fired += it.size }
        }
        due.forEach { it.done.complete(Unit) }
    }

    fun skewWallClock(seconds: Double) = synchronized(lock) { skew += seconds }

    fun pendingSeconds(): List<Double> = synchronized(lock) { sleepers.map { it.seconds } }

    fun pendingDues(): List<Double> = synchronized(lock) { sleepers.map { it.due } }

    fun pending(seconds: Double): Int = synchronized(lock) { sleepers.count { it.seconds == seconds } }
}
