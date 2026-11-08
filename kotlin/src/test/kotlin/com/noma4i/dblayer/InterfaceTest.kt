package com.noma4i.dblayer

import com.noma4i.dblayer.models.InterfaceRecord
import com.noma4i.dblayer.models.SampleAction
import com.noma4i.dblayer.models.SampleButton
import com.noma4i.dblayer.models.SampleCondition
import com.noma4i.dblayer.models.SampleElement
import com.noma4i.dblayer.models.SampleElementKind
import com.noma4i.dblayer.models.SampleFlagLiteral
import com.noma4i.dblayer.models.SampleFlagValue
import com.noma4i.dblayer.models.SampleNavigateAction
import com.noma4i.dblayer.models.SampleNumberLiteral
import com.noma4i.dblayer.models.SampleNumberValue
import com.noma4i.dblayer.models.SampleOpenRecordAction
import com.noma4i.dblayer.models.SampleOpenExternalAction
import com.noma4i.dblayer.models.SampleOpenProfileAction
import com.noma4i.dblayer.models.SampleOperand
import com.noma4i.dblayer.models.SamplePressable
import com.noma4i.dblayer.models.SampleSpacer
import com.noma4i.dblayer.models.SampleSpec
import com.noma4i.dblayer.models.SampleStateEntry
import com.noma4i.dblayer.models.SampleStateRef
import com.noma4i.dblayer.models.SampleStateValue
import com.noma4i.dblayer.models.SampleTextLiteral
import com.noma4i.dblayer.models.SampleTextValue
import com.noma4i.dblayer.models.SampleVisibility
import com.noma4i.dblayer.models.SampleVisibleAll
import com.noma4i.dblayer.models.SampleVisibleFlag
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class InterfaceTest {
    private val document = "query SampleNode(\$id: ID!) { node(id: \$id) { ... on InterfaceRecord { id slug spec { root " +
        "elements { __typename id children press { __typename ... on SampleNavigateAction { slug title } ... on SampleOpenRecordAction { recordId } ... on SampleOpenExternalAction { url } ... on SampleOpenProfileAction { userId } } " +
        "visible { __typename ... on SampleCondition { comparison negated operand { __typename ... on SampleTextLiteral { text } ... on SampleStateRef { path } } path } ... on SampleVisibleAll { conditions { comparison negated path } } ... on SampleVisibleFlag { flag } } " +
        "... on SampleButton { label { __typename ... on SampleStateRef { path } ... on SampleTextLiteral { text } } disabled { __typename ... on SampleFlagLiteral { flag } } } " +
        "... on SampleColumn { gap { __typename ... on SampleNumberLiteral { number } } alignItems } ... on SampleHeading { text { __typename ... on SampleTextLiteral { text } } level } " +
        "... on SampleSpacer { length { __typename ... on SampleNumberLiteral { number } } } ... on SampleSwitch { checked { __typename ... on SampleStateRef { path } } label { __typename ... on SampleTextLiteral { text } } } } " +
        "state { path value { __typename ... on SampleTextLiteral { text } ... on SampleNumberLiteral { number } } } } } } }"

    private val firstElements = """
        [
          {"__typename":"SampleColumn","id":"root","children":["b1","h1","p1","sw1","sp1","f1"],"press":null,"visible":{"__typename":"SampleVisibleAll","conditions":[{"comparison":"EQ","negated":false,"operand":{"__typename":"SampleTextLiteral","text":"x"},"path":"user.name"}]},"gap":{"__typename":"SampleNumberLiteral","number":8},"alignItems":"center"},
          {"__typename":"SampleButton","id":"b1","children":[],"press":{"__typename":"SampleNavigateAction","slug":"home","title":"Home"},"visible":null,"label":{"__typename":"SampleStateRef","path":"user.name"},"disabled":{"__typename":"SampleFlagLiteral","flag":true}},
          {"__typename":"SampleHeading","id":"h1","children":[],"press":null,"visible":{"__typename":"SampleVisibleFlag","flag":true},"text":{"__typename":"SampleTextLiteral","text":"Hi"},"level":2},
          {"__typename":"SamplePressable","id":"p1","children":["h1"],"press":{"__typename":"SampleOpenRecordAction","recordId":"c9"},"visible":{"__typename":"SampleCondition","comparison":"NE","negated":true,"operand":{"__typename":"SampleStateRef","path":"flag"},"path":"a.b"}},
          {"__typename":"SampleSwitch","id":"sw1","children":[],"press":null,"visible":null,"checked":{"__typename":"SampleStateRef","path":"on"},"label":null},
          {"__typename":"SampleSpacer","id":"sp1","children":[],"press":null,"visible":null,"length":{"__typename":"SampleNumberLiteral","number":4.5}},
          {"__typename":"SampleFuture","id":"f1","children":["x"],"size":3}
        ]
    """.trimIndent()

    private fun reply(elements: String, state: String = """[{"path":"user.name","value":{"__typename":"SampleTextLiteral","text":"Ada"}},{"path":"n","value":{"__typename":"SampleNumberLiteral","number":3}}]""") =
        """{"data":{"node":{"__typename":"InterfaceRecord","id":"s1","slug":"main","spec":{"root":"root","elements":$elements,"state":$state}}}}"""

    private suspend fun openRig(): MockTransport {
        val transport = MockTransport()
        openSigned(Config(null, null), modelsJSON, transport)
        DBlayer.database.registerOperations(JsonArray(listOf(buildJsonObject { put("name", JsonPrimitive("SampleNode")); put("document", JsonPrimitive(document)) })).toString())
        return transport
    }

    @Test
    fun aServerAnswerCarriesEveryElementVariantAndALaterAnswerReplacesTheList() = bounded {
        val transport = openRig()
        transport.enqueue("SampleNode", reply(firstElements))
        val spec = InterfaceRecord.node("s1")!!.spec!!
        assertEquals("root", spec.root)
        assertEquals(listOf("root", "b1", "h1", "p1", "sw1", "sp1", null), spec.elements.map { it.id })
        assertEquals(
            listOf(SampleElementKind.Column, SampleElementKind.Button, SampleElementKind.Heading, SampleElementKind.Pressable, SampleElementKind.Switch, SampleElementKind.Spacer, null),
            spec.elements.map { it.kind },
        )
        assertEquals(listOf("b1", "h1", "p1", "sw1", "sp1", "f1"), spec.elements[0].children)
        assertEquals(listOf("h1"), spec.elements[3].children)
        assertEquals(SampleElement.Unknown("SampleFuture"), spec.elements[6])
        val button = (spec.elements[1] as SampleElement.Button).value
        assertEquals(SampleAction.Navigate(SampleNavigateAction(slug = "home", title = "Home")), button.press)
        assertEquals(SampleTextValue.StateRef(SampleStateRef(path = "user.name")), button.label)
        assertEquals(SampleFlagValue.FlagLiteral(SampleFlagLiteral(flag = true)), button.disabled)
        val column = (spec.elements[0] as SampleElement.Column).value
        assertEquals(SampleNumberValue.NumberLiteral(SampleNumberLiteral(number = 8.0)), column.gap)
        assertEquals("center", column.alignItems)
        assertEquals(
            SampleVisibility.VisibleAll(SampleVisibleAll(conditions = listOf(SampleCondition(comparison = "EQ", negated = false, operand = SampleOperand.TextLiteral(SampleTextLiteral(text = "x")), path = "user.name")))),
            column.visible,
        )
        assertEquals(SampleAction.OpenRecord(SampleOpenRecordAction(recordId = "c9")), spec.elements[3].press)
        assertEquals(
            SampleVisibility.Condition(SampleCondition(comparison = "NE", negated = true, operand = SampleOperand.StateRef(SampleStateRef(path = "flag")), path = "a.b")),
            spec.elements[3].visible,
        )
        assertEquals(SampleVisibility.VisibleFlag(SampleVisibleFlag(flag = true)), spec.elements[2].visible)
        assertEquals(
            listOf(
                SampleStateEntry(path = "user.name", value = SampleStateValue.Text(SampleTextLiteral(text = "Ada"))),
                SampleStateEntry(path = "n", value = SampleStateValue.Number(SampleNumberLiteral(number = 3.0))),
            ),
            spec.state,
        )

        val second = """
            [
              {"__typename":"SampleHeading","id":"b1","children":["q"],"press":{"__typename":"SampleOpenExternalAction","url":"https://x"},"visible":null,"text":{"__typename":"SampleTextLiteral","text":"now a heading"},"level":null},
              {"__typename":"SampleColumn","id":"root","children":["b1"],"press":null,"visible":null,"gap":null,"alignItems":null}
            ]
        """.trimIndent()
        transport.enqueue("SampleNode", reply(second, "[]"))
        InterfaceRecord.node("s1", force = true)
        val again = InterfaceRecord.find("s1").current()!!.spec!!
        assertEquals(listOf("b1", "root"), again.elements.map { it.id })
        assertEquals(listOf(SampleElementKind.Heading, SampleElementKind.Column), again.elements.map { it.kind })
        assertEquals(listOf("q"), again.elements[0].children)
        assertEquals(SampleAction.OpenExternal(SampleOpenExternalAction(url = "https://x")), again.elements[0].press)
        assertNull(again.elements[1].visible)
        assertTrue(again.state.isEmpty())
    }

    @Test
    fun aSampleWrittenWithTypedElementsReadsBackAndAnElementCanChangeItsVariant() = bounded {
        openRig()
        val button = SampleElement.Button(SampleButton(id = "b1", children = emptyList(), press = SampleAction.OpenProfile(SampleOpenProfileAction(userId = "u1")), label = SampleTextValue.TextLiteral(SampleTextLiteral(text = "Go"))))
        val pressable = SampleElement.Pressable(SamplePressable(id = "p1", children = listOf("b1")))
        val elements = listOf(button, pressable, SampleElement.Unknown("SampleFuture"))
        val created = InterfaceRecord.create(slug = "local", spec = SampleSpec(root = "b1", elements = elements, state = emptyList())).optimisticValue!!
        assertEquals(elements, created.spec!!.elements)

        val spacer = SampleElement.Spacer(SampleSpacer(id = "b1", children = emptyList(), length = SampleNumberValue.NumberLiteral(SampleNumberLiteral(number = 2.0))))
        val changed = created.update(spec = GraphQLNullable.Value(SampleSpec(root = "b1", elements = listOf(spacer), state = emptyList()))).optimisticValue!!
        assertEquals(listOf(spacer), changed.spec!!.elements)
        assertEquals(listOf(spacer), InterfaceRecord.find(created.id).current()!!.spec!!.elements)
    }
}
