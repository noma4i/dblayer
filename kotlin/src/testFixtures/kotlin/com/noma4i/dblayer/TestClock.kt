package com.noma4i.dblayer

@RequiresOptIn(level = RequiresOptIn.Level.ERROR)
annotation class DBlayerTestingApi

private val opener = OpenDatabase()

private class OnlineAtOnce : PlatformObserver {
    override fun `start`(sink: OnlineSink) { sink.setOnline(true) }
    override fun `stop`() {}
}

/** Opens the database on a clock that only `advance` moves, so freshness windows, retry delays and expiry run without sleeping. The platform reports online at once unless the test passes its own `observers`. */
@DBlayerTestingApi
suspend fun DBlayer.openTesting(transport: Transport?, modelsJson: String, path: String? = null, operations: List<GraphQLOperation> = emptyList(), startMillis: Long = System.currentTimeMillis(), encrypted: EncryptedConfig? = null, credentials: CredentialStore? = null, http: EncryptedHttp? = null, observers: PlatformObserver? = null): TestClock {
    val clock = TestClock(startMillis.toULong())
    val database = opener.openWithClock(Config(path, null), modelsJson, OpenPorts(transport, encrypted, credentials, http, SCRIPTED_ENDPOINT), clock, OpenOptions(operations.map { OperationDefinition(it.operationName, it.document) }), observers ?: OnlineAtOnce())
    adopt(database)
    ChangePump.start(database)
    return clock
}

@DBlayerTestingApi
fun TestClock.advance(millis: Long) {
    advance(millis.toULong())
    TestPulse.pulse()
}
