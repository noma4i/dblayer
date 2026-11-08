
internal object ProcessOwner {
    private const val KEY = "com.noma4i.dblayer.loader"

    fun claim() {
        val loader = ProcessOwner::class.java.classLoader
        val properties = System.getProperties()
        val owner = properties.putIfAbsent(KEY, loader ?: Any())
        check(owner == null || owner === loader) { "DBlayer is already loaded by another class loader in this process; load it once" }
    }
}

internal object RuntimeLane {
    private val executor = java.util.concurrent.Executors.newSingleThreadExecutor { task -> Thread(task, "dblayer-runtime-lane").apply { isDaemon = true } }

    fun enqueue(work: () -> Unit) {
        executor.execute(work)
    }
}
