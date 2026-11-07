package com.noma4i.dblayer

import com.noma4i.dblayer.models.NestedSample
import com.noma4i.dblayer.models.NestedValue
import com.noma4i.dblayer.models.NestedSamplePhase
import com.noma4i.dblayer.models.NestedValueState
import com.noma4i.dblayer.models.Vibe
import com.noma4i.dblayer.models.VibeKind
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.builtins.ListSerializer
import org.junit.Assert.assertEquals
import org.junit.Test

class EnumTest {
    private suspend fun open() = openDatabase(Config(null, null), modelsJSON, null).also { adopt(it) }

    @Test
    fun anUnknownValueDecodesWithoutLossAndEncodesBackAsItArrived() {
        val serializer = ListSerializer(VibeKind.serializer())
        val decoded = dbJson.decodeFromString(serializer, """["like","voice_note"]""")
        assertEquals(listOf(VibeKind.Like, VibeKind.Unknown("voice_note")), decoded)
        assertEquals("""["like","voice_note"]""", dbJson.encodeToString(serializer, decoded))
        assertEquals(VibeKind.Love, VibeKind.of("love"))
    }

    @Test
    fun aValueOutsideTheDeclaredOnesIsStoredReadBackAndKeptByAnEdit() = bounded {
        val database = open()
        val wire = database.create(database.sessionEpoch(), "NestedSample", """{"phase":"archived"}""", "{}").optimisticValue!!
        val sample = NestedSample.read(wire)
        assertEquals(NestedSamplePhase.Unknown("archived"), sample.phase)
        val edited = sample.update(values = emptyList()).optimisticValue!!
        assertEquals(NestedSamplePhase.Unknown("archived"), edited.phase)
        val matching = withTimeout(5000) { NestedSample.where(NestedSample.phase eq NestedSamplePhase.Unknown("archived")).snapshots.flow.first { it.rows.isNotEmpty() } }
        assertEquals(listOf(sample.id), matching.rows.map { it.id })
        val declared = withTimeout(5000) { NestedSample.where(NestedSample.phase eq NestedSamplePhase.Live).snapshots.flow.first() }
        assertEquals(emptyList<String>(), declared.rows.map { it.id })
    }

    @Test
    fun theTypedApiWritesADeclaredCaseAndQueriesCompareTypedValues() = bounded {
        open()
        val sample = NestedSample.create(phase = NestedSamplePhase.Live).optimisticValue!!
        assertEquals(NestedSamplePhase.Live, sample.phase)
        val live = withTimeout(5000) { NestedSample.where(NestedSample.phase eq NestedSamplePhase.Live).snapshots.flow.first { it.rows.isNotEmpty() } }
        assertEquals(listOf(sample.id), live.rows.map { it.id })
        assertEquals(NestedSamplePhase.Unknown("later"), sample.update(phase = GraphQLNullable.Value(NestedSamplePhase.Unknown("later"))).optimisticValue!!.phase)
    }

    @Test
    fun aNestedEnumKeepsAValueTheDeclarationLacksOnBothDecodePaths() = bounded {
        val decoded = dbJson.decodeFromString(NestedValue.serializer(), """{"title":"t","state":"mystery"}""")
        assertEquals(NestedValueState.Unknown("mystery"), decoded.state)
        val wire = open().let { it.create(it.sessionEpoch(), "NestedSample", """{"value":{"title":"t","state":"mystery"}}""", "{}").optimisticValue!! }
        val sample = NestedSample.read(wire)
        assertEquals(NestedValueState.Unknown("mystery"), sample.value?.state)
        assertEquals("""mystery""", jsonValue(sample.value!!).toString().substringAfter("\"state\":\"").substringBefore("\""))
    }
}
