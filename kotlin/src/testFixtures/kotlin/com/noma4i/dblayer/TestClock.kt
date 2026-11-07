package com.noma4i.dblayer

@RequiresOptIn(level = RequiresOptIn.Level.ERROR)
annotation class DBlayerTestingApi

private val opener = OpenDatabase()

/** Opens the database on a clock that only `advance` moves, so freshness windows, retry delays and expiry run without sleeping. */
@DBlayerTestingApi
suspend fun DBlayer.openTesting(transport: Transport?, modelsJson: String, path: String? = null, operations: List<GraphQLOperation> = emptyList(), startMillis: Long = System.currentTimeMillis(), encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, observers: PlatformObserver? = null): TestClock {
    val clock = TestClock(startMillis.toULong())
    adopt(opener.openWithClock(Config(path, null), modelsJson, OpenPorts(transport, encrypted, credentials, http, SCRIPTED_ENDPOINT), clock, OpenOptions(operations.map { OperationDefinition(it.operationName, it.document) }, null), observers))
    return clock
}

@DBlayerTestingApi
fun TestClock.advance(millis: Long) = advance(millis.toULong())
