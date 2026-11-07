package com.noma4i.dblayer

@RequiresOptIn(level = RequiresOptIn.Level.ERROR)
annotation class DBlayerTestingApi

/** Opens the database on a clock that only `advance` moves, so freshness windows, retry delays and expiry run without sleeping. */
@DBlayerTestingApi
suspend fun DBlayer.openTesting(transport: Transport?, modelsJson: String, path: String? = null, operations: List<GraphQLOperation> = emptyList(), startMillis: Long = System.currentTimeMillis(), encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null): TestClock {
    val clock = TestClock(startMillis.toULong())
    install(openOnClock(Config(path, null), modelsJson, transport, SCRIPTED_ENDPOINT, clock, encrypted, credentials, http), transport, operations)
    return clock
}

@DBlayerTestingApi
fun TestClock.advance(millis: Long) = advance(millis.toULong())
