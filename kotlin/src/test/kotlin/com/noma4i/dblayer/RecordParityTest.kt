package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.ScalarProbe
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/** bridge_prop_record_parity (P-BR-05): a value written through the typed API comes back through the optimistic row, the row read and the snapshot stream as it was written. */
class RecordParityTest {
    private data class Values(
        val text: String? = null,
        val count: Long? = null,
        val ratio: Double? = null,
        val flag: Boolean? = null,
        val at: String? = null,
        val blob: JsonElement? = null,
    ) {
        companion object {
            fun of(row: ScalarProbe) = Values(row.text, row.count, row.ratio, row.flag, row.at, row.blob)
        }
    }

    private fun points(vararg codePoints: Int): String = String(codePoints, 0, codePoints.size)

    private val wideBlob: JsonElement = JsonObject(
        mapOf(
            "max" to JsonPrimitive(Long.MAX_VALUE),
            "min" to JsonPrimitive(Long.MIN_VALUE),
            "beyondDouble" to JsonPrimitive(9_007_199_254_740_993L),
            "real" to JsonPrimitive(1.5),
            "flag" to JsonPrimitive(false),
            "none" to JsonNull,
            "list" to JsonArray(listOf(JsonPrimitive(0), JsonPrimitive(""), JsonArray(emptyList()), JsonObject(emptyMap()))),
            "text" to JsonPrimitive("日本 🎉"),
        ),
    )

    private val probeCases: List<Pair<String, Values>> = listOf(
        "none in every field" to Values(),
        "zero in every kind" to Values("", 0L, 0.0, false, "1970-01-01T00:00:00.000Z", JsonPrimitive(0)),
        "upper bounds and unicode" to Values(points(0x451, 0x436) + " 日本 🎉 e" + points(0x301) + " \"q\" \\ \n\t", Long.MAX_VALUE, Double.MAX_VALUE, true, "9999-12-31T23:59:59.999Z", wideBlob),
        "lower bounds and a long text" to Values("x".repeat(65_536), Long.MIN_VALUE, -Double.MAX_VALUE, false, "0001-01-01T00:00:00.000Z", JsonPrimitive("")),
        "smallest real and the first integer beyond a double" to Values(count = 9_007_199_254_740_993L, ratio = java.lang.Double.MIN_VALUE, blob = JsonArray(emptyList())),
        "negative integer and an empty object" to Values(count = -1L, ratio = -1.5, flag = true, blob = JsonObject(emptyMap())),
        "a boolean json" to Values(blob = JsonPrimitive(false)),
    )

    private class FileCase(val label: String, val bytes: ByteArray, val name: String, val mime: String)

    private val fileCases: List<FileCase> = listOf(
        FileCase("empty bytes", ByteArray(0), "empty.bin", "application/octet-stream"),
        FileCase("one byte and a unicode name", ByteArray(1), points(0x444, 0x43E, 0x442, 0x43E, 0x20, 0x1F389) + ".png", "image/png"),
        FileCase("one mebibyte", ByteArray(1 shl 20) { 0xAB.toByte() }, "big.bin", "application/octet-stream"),
    )

    private val failureOperations = """[{"name":"CreateMoment","document":"mutation CreateMoment(${'$'}text: String) { createMoment(text: ${'$'}text) { id uuid likedByMe media createdAt } }"}]"""

    private suspend fun create(values: Values) = ScalarProbe.create(values.text, values.count, values.ratio, values.flag, values.at, values.blob)

    private fun <T : Any> nullable(value: T?): GraphQLNullable<T> = if (value == null) GraphQLNullable.Null else GraphQLNullable.Value(value)

    private suspend fun update(row: ScalarProbe, values: Values) =
        row.update(nullable(values.text), nullable(values.count), nullable(values.ratio), nullable(values.flag), nullable(values.at), nullable(values.blob))

    private suspend fun expectParity(id: String, optimistic: ScalarProbe?, expected: Values, label: String) {
        assertEquals(label, expected, optimistic?.let(Values::of))
        assertEquals(label, expected, ScalarProbe.find(id).current()?.let(Values::of))
        val snapshot = withTimeout(5000) { ScalarProbe.ids(listOf(id)).snapshots.flow.first { it.rows.isNotEmpty() } }
        assertEquals(label, expected, snapshot.rows.first().let(Values::of))
    }

    @Test
    fun bridge_prop_record_parity_everyScalarKindKeepsItsValueThroughCreateExplicitNoneAndUpdate() = bounded(60_000) {
        openSigned(Config(null), modelsJSON, null)
        for ((label, values) in probeCases) {
            val created = create(values).optimisticValue!!
            expectParity(created.id, created, values, "create: $label")

            val cleared = update(created, Values()).optimisticValue!!
            expectParity(created.id, cleared, Values(), "update to None: $label")

            val restored = update(cleared, values).optimisticValue!!
            expectParity(created.id, restored, values, "update back: $label")
        }
    }

    /** The core keeps one zero: a negative zero is stored and read as the positive one, on both platforms (value_tests.rs number_edge_cases_normalize). */
    @Test
    fun bridge_prop_record_parity_aNegativeZeroIsStoredAsThePositiveZero() = bounded {
        openSigned(Config(null), modelsJSON, null)
        val created = create(Values(ratio = -0.0)).optimisticValue!!
        assertEquals(0.0.toRawBits(), created.ratio!!.toRawBits())
        assertEquals(0.0.toRawBits(), ScalarProbe.find(created.id).current()!!.ratio!!.toRawBits())
    }

    @Test
    fun bridge_prop_record_parity_aFileKeepsItsSizeNameMimeAndDigestThroughAttachReadRowAndSnapshot() = bounded(60_000) {
        openSigned(Config(null), modelsJSON, null)
        val probe = create(Values()).optimisticValue!!
        assertNull(probe.file(ScalarProbe.file))
        for (case in fileCases) {
            val source = java.io.File.createTempFile("attachment", ".bin")
            source.deleteOnExit()
            source.writeBytes(case.bytes)
            val info = probe.attach(ScalarProbe.file, source, case.name, case.mime)
            assertEquals(case.label, Triple(case.name, case.mime, case.bytes.size.toULong()), Triple(info.name, info.mime, info.size))
            assertEquals(case.label, info, probe.file(ScalarProbe.file))
            assertArrayEquals(case.label, case.bytes, java.io.File(requireNotNull(probe.url(ScalarProbe.file))).readBytes())
            assertEquals(case.label, info, ScalarProbe.find(probe.id).current()?.file)
            val snapshot = withTimeout(5000) { ScalarProbe.ids(listOf(probe.id)).snapshots.flow.first { it.rows.firstOrNull()?.file == info } }
            assertNotNull(case.label, snapshot)
        }
        assertTrue(probe.detach(ScalarProbe.file))
        assertNull(ScalarProbe.find(probe.id).current()?.file)
        withTimeout(5000) { ScalarProbe.ids(listOf(probe.id)).snapshots.flow.first { it.rows.isNotEmpty() && it.rows.first().file == null } }
        Unit
    }

    @Test
    fun bridge_prop_record_parity_aRefusalKeepsTheSessionCodeTheRequestSizeAndTheLimitOrTheirAbsence() = bounded(60_000) {
        class Case(val label: String, val text: String, val failure: DbException?, val code: SessionCode?, val oversize: Boolean)
        val cases = listOf(
            Case("a request over the size limit is refused locally with its size and limit", "x".repeat(300_000), null, null, true),
            Case("a session code arrives without a size and a limit", "hello", DbException.Session(SessionCode.RATE_LIMITED, "slow down"), SessionCode.RATE_LIMITED, false),
        )
        for (case in cases) {
            val transport = MockTransport()
            openSigned(Config(null), modelsJSON, transport)
            registerStandOperations(DBlayer.database, failureOperations)
            case.failure?.let { transport.enqueue("CreateMoment", it) }
            val refused = withTimeout(5000) { Moment.create(text = case.text, media = null, account = AccountScope.current()).refusal() }
            assertEquals(case.label, case.code, refused.sessionCode)
            if (case.oversize) {
                assertEquals(case.label, 256UL * 1024UL, refused.requestLimit)
                assertTrue(case.label, (refused.requestSize ?: 0UL) > 256UL * 1024UL)
            } else {
                assertTrue(case.label, refused.requestSize == null && refused.requestLimit == null)
            }
            assertTrue(case.label, refused.message.isNotEmpty())
            val restored = OperationFailure(refused.kind, refused.sessionCode, refused.requestSize, refused.requestLimit, refused.message, refused.extensions)
            assertEquals(case.label, restored, dbJson.decodeFromString(OperationFailure.serializer(), dbJson.encodeToString(OperationFailure.serializer(), restored)))
        }
    }
}
