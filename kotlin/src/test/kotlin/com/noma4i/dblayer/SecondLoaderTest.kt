package com.noma4i.dblayer

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

private class CopyingLoader(private val source: ClassLoader) : ClassLoader(source) {
    override fun loadClass(name: String, resolve: Boolean): Class<*> {
        if (!name.startsWith("com.noma4i.dblayer.") || name.startsWith("com.noma4i.dblayer.models.")) return super.loadClass(name, resolve)
        synchronized(this) {
            findLoadedClass(name)?.let { return it }
            val bytes = source.getResourceAsStream(name.replace('.', '/') + ".class")?.use { it.readBytes() } ?: return super.loadClass(name, resolve)
            return defineClass(name, bytes, 0, bytes.size)
        }
    }
}

class SecondLoaderTest {
    @Test
    fun aSecondLoaderIsRefused() = bounded {
        uniffiEnsureInitialized()
        val second = CopyingLoader(SecondLoaderTest::class.java.classLoader)
        val refusal = runCatching { second.loadClass("com.noma4i.dblayer.DblayerKt").getMethod("uniffiEnsureInitialized").invoke(null) }.exceptionOrNull()
        val cause = generateSequence(refusal) { it.cause }.last()
        assertTrue(cause is IllegalStateException)
        assertEquals("DBlayer is already loaded by another class loader in this process; load it once", cause.message)
    }
}
