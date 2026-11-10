package com.noma4i.dblayer

import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.modelsJSON
import java.io.File
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class FileFieldTest {
    @Test
    fun aFileIsAttachedReadLocatedAndDetachedThroughTheRecord() = bounded {
        openSigned(Config(null, null), modelsJSON, null)
        val user = User.create(name = "ann").optimisticValue!!
        assertNull(user.file(User.avatar))

        val bytes = byteArrayOf(1, 2, 3, 4)
        val source = File.createTempFile("attachment", ".bin")
        source.deleteOnExit()
        source.writeBytes(bytes)
        val info = user.attach(User.avatar, source, "a.png", "image/png")
        assertEquals(Triple("a.png", "image/png", 4UL), Triple(info.name, info.mime, info.size))
        assertEquals(info, user.file(User.avatar))
        assertArrayEquals(bytes, File(user.url(User.avatar)!!).readBytes())
        assertEquals(info, User.find(user.id).current()!!.avatar)
        User.find(user.id).current()!!.let { assertTrue("bridge_prop_projection_revision: the row keeps the metadata of its wire row", it.rowRevision > 0 && it.estimatedBytes > 0u) }

        source.writeBytes(byteArrayOf(9))
        val other = user.attach(User.avatar, source, "b.png", "image/png")
        assertArrayEquals(byteArrayOf(9), File(user.url(User.avatar)!!).readBytes())
        assertNotEquals(info.id, other.id)

        assertTrue(user.detach(User.avatar))
        assertNull(user.file(User.avatar))
        assertNull(user.url(User.avatar))
    }
}
