package com.noma4i.dblayer

internal object ProcessOwner {
    private const val KEY = "com.noma4i.dblayer.loader"

    fun claim() {
        val loader = ProcessOwner::class.java.classLoader
        val properties = System.getProperties()
        synchronized(properties) {
            val owner = properties.putIfAbsent(KEY, loader ?: Any())
            check(owner == null || owner === loader) { "DBlayer is already loaded by another class loader in this process; load it once" }
        }
    }
}
