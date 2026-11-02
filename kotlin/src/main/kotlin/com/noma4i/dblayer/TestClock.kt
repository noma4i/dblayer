package com.noma4i.dblayer

@RequiresOptIn(level = RequiresOptIn.Level.ERROR)
annotation class DBlayerTestingApi

@DBlayerTestingApi
suspend fun DBlayer.resetForTesting() = database.resetForTesting()

@DBlayerTestingApi
suspend fun DBlayer.settleForTesting() = database.settleForTesting()

@DBlayerTestingApi
fun DBlayer.enterForegroundForTesting() = database.enterForegroundForTesting()

/** Opens the database on a clock that only `advance` moves, so freshness windows, retry delays and expiry run without sleeping. Scope preloads send no request unless `preload` is true. */
@DBlayerTestingApi
suspend fun DBlayer.openTesting(transport: Transport?, modelsJson: String, path: String? = null, operations: List<GraphQLOperation> = emptyList(), uploads: UploadOperations? = null, startMillis: Long = System.currentTimeMillis(), preload: Boolean = false, listening: Boolean = true): TestClock {
    val clock = TestClock(startMillis.toULong())
    install(openOnClock(Config(path, null, 2000u), modelsJson, transport, uploads, clock, preload, listening), transport, operations)
    return clock
}

@DBlayerTestingApi
fun TestClock.advance(millis: Long) = advance(millis.toULong())
