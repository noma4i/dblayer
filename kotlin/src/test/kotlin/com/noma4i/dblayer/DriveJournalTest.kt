package com.noma4i.dblayer

import java.io.File
import java.nio.file.Files
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class DriveJournalTest {
    @Test
    fun compactsOnlyAtTheBoundAndKeepsTheOriginal() = bounded {
        val directory = Files.createTempDirectory("drive-journal").toFile()
        try {
            val key = "a".repeat(64)
            val current = File(directory, "committed")
            val appended = File(directory, "appended")
            val compacted = File(directory, "compacted")
            val entries = listOf("aa", "bb", "cc").map { DriveJournalEntry(key, it.toByteArray()) }
            DriveJournal.prepare(null, current, listOf(entries[0]))
            val original = current.readBytes()
            DriveJournal.prepare(current, appended, listOf(entries[1]))
            assertTrue(appended.length() > current.length())
            assertArrayEquals(original, current.readBytes())

            DriveJournal.prepare(appended, compacted, listOf(entries[2]))
            assertEquals(current.length(), compacted.length())
            val snapshot = mutableListOf<DriveJournalEntry>()
            DriveJournal.readLatest(compacted) { snapshot.add(it) }
            assertEquals(key, snapshot.single().key)
            assertArrayEquals(entries[2].payload, snapshot.single().payload)

            val bytes = compacted.readBytes()
            compacted.writeBytes(bytes.copyOf(bytes.size - 1))
            assertTrue(runCatching { DriveJournal.readLatest(compacted) { error("truncated record was delivered") } }.isFailure)
        } finally {
            directory.deleteRecursively()
        }
    }
}
