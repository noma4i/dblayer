package com.noma4i.dblayer

import java.io.File
import java.io.RandomAccessFile
import java.nio.file.Files

internal data class DriveJournalEntry(val key: String, val payload: ByteArray)

internal object DriveJournal {
    private const val HEADER_SIZE = 5L
    private const val FRAME_SIZE = 68L
    private const val MAX_BODY = 900000
    private val header = byteArrayOf(68, 66, 76, 74, 1)

    private data class Frame(val offset: Long, val size: Int, val change: Int = -1)

    fun prepare(current: File?, candidate: File, changes: List<DriveJournalEntry>) {
        require(current == null || current.canonicalFile != candidate.canonicalFile)
        require(current == null || !candidate.exists() || !Files.isSameFile(current.toPath(), candidate.toPath()))
        changes.forEach { validate(it.key, it.payload.size) }
        val source = current?.let { RandomAccessFile(it, "r") }
        try {
            val index = source?.let { index(it) } ?: linkedMapOf()
            var historySize = source?.length() ?: HEADER_SIZE
            changes.forEachIndexed { position, entry ->
                index[entry.key] = Frame(0, entry.payload.size, position)
                historySize = Math.addExact(historySize, FRAME_SIZE + entry.payload.size)
            }
            val snapshotSize = index.values.fold(HEADER_SIZE) { size, frame ->
                Math.addExact(size, FRAME_SIZE + frame.size)
            }
            val compact = historySize - snapshotSize >= snapshotSize
            RandomAccessFile(candidate, "rw").use { output ->
                output.setLength(0)
                if (compact) {
                    output.write(header)
                    index.forEach { (key, frame) ->
                        if (frame.change >= 0) {
                            write(output, changes[frame.change])
                        } else {
                            source!!.seek(frame.offset)
                            copy(source, output, FRAME_SIZE + frame.size)
                        }
                    }
                } else {
                    if (source == null) {
                        output.write(header)
                    } else {
                        source.seek(0)
                        copy(source, output, source.length())
                    }
                    changes.forEach { write(output, it) }
                }
            }
        } finally {
            source?.close()
        }
    }

    fun readLatest(file: File, consume: (DriveJournalEntry) -> Unit) {
        RandomAccessFile(file, "r").use { input ->
            val index = index(input)
            index.forEach { (key, frame) ->
                input.seek(frame.offset + FRAME_SIZE)
                val body = ByteArray(frame.size)
                input.readFully(body)
                consume(DriveJournalEntry(key, body))
            }
        }
    }

    private fun index(input: RandomAccessFile): LinkedHashMap<String, Frame> {
        require(input.length() >= HEADER_SIZE) { "Invalid journal header" }
        input.seek(0)
        val actual = ByteArray(header.size)
        input.readFully(actual)
        require(actual.contentEquals(header)) { "Invalid journal header" }
        val frames = linkedMapOf<String, Frame>()
        while (input.filePointer < input.length()) {
            val offset = input.filePointer
            require(input.length() - offset >= FRAME_SIZE) { "Incomplete journal frame" }
            val keyBytes = ByteArray(64)
            input.readFully(keyBytes)
            val key = keyBytes.toString(Charsets.US_ASCII)
            val size = input.readInt()
            validate(key, size)
            require(input.length() - input.filePointer >= size) { "Incomplete journal body" }
            frames[key] = Frame(offset, size)
            input.seek(input.filePointer + size)
        }
        return frames
    }

    private fun validate(key: String, size: Int) {
        require(key.length == 64 && key.all { it in '0'..'9' || it in 'a'..'f' }) { "Invalid journal key" }
        require(size in 1..MAX_BODY) { "Invalid journal body size" }
    }

    private fun write(output: RandomAccessFile, entry: DriveJournalEntry) {
        output.write(entry.key.toByteArray(Charsets.US_ASCII))
        output.writeInt(entry.payload.size)
        output.write(entry.payload)
    }

    private fun copy(input: RandomAccessFile, output: RandomAccessFile, count: Long) {
        val buffer = ByteArray(8192)
        var remaining = count
        while (remaining > 0) {
            val length = minOf(remaining, buffer.size.toLong()).toInt()
            input.readFully(buffer, 0, length)
            output.write(buffer, 0, length)
            remaining -= length
        }
    }
}
