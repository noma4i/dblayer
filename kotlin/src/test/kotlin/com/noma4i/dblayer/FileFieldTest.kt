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
        val info = user.attach(User.avatar, bytes, "a.png", "image/png")
        assertEquals(Triple("a.png", "image/png", 4L), Triple(info.name, info.mime, info.size))
        assertEquals(info, user.file(User.avatar))
        assertArrayEquals(bytes, user.read(User.avatar))
        assertArrayEquals(bytes, File(user.url(User.avatar)!!).readBytes())
        assertEquals(info, User.find(user.id).current()!!.avatar)

        val other = user.attach(User.avatar, byteArrayOf(9), "b.png", "image/png")
        assertArrayEquals(byteArrayOf(9), user.read(User.avatar))
        assertNotEquals(info.id, other.id)

        assertTrue(user.detach(User.avatar))
        assertNull(user.file(User.avatar))
        assertNull(user.read(User.avatar))
    }
}
