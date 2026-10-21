package com.noma4i.dblayer

import com.noma4i.dblayer.models.CustomScreen
import com.noma4i.dblayer.models.ScreenAction
import com.noma4i.dblayer.models.ScreenButton
import com.noma4i.dblayer.models.ScreenCondition
import com.noma4i.dblayer.models.ScreenElement
import com.noma4i.dblayer.models.ScreenElementKind
import com.noma4i.dblayer.models.ScreenFlagLiteral
import com.noma4i.dblayer.models.ScreenFlagValue
import com.noma4i.dblayer.models.ScreenNavigateAction
import com.noma4i.dblayer.models.ScreenNumberLiteral
import com.noma4i.dblayer.models.ScreenNumberValue
import com.noma4i.dblayer.models.ScreenOpenChatAction
import com.noma4i.dblayer.models.ScreenOpenExternalAction
import com.noma4i.dblayer.models.ScreenOpenProfileAction
import com.noma4i.dblayer.models.ScreenOperand
import com.noma4i.dblayer.models.ScreenPressable
import com.noma4i.dblayer.models.ScreenSpacer
import com.noma4i.dblayer.models.ScreenSpec
import com.noma4i.dblayer.models.ScreenStateEntry
import com.noma4i.dblayer.models.ScreenStateRef
import com.noma4i.dblayer.models.ScreenStateValue
import com.noma4i.dblayer.models.ScreenTextLiteral
import com.noma4i.dblayer.models.ScreenTextValue
import com.noma4i.dblayer.models.ScreenVisibility
import com.noma4i.dblayer.models.ScreenVisibleAll
import com.noma4i.dblayer.models.ScreenVisibleFlag
import com.noma4i.dblayer.models.modelsJSON
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class InterfaceTest {
    private val document = "query ScreenNode(\$id: ID!) { node(id: \$id) { ... on CustomScreen { id slug spec { root " +
        "elements { __typename id children press { __typename ... on ScreenNavigateAction { slug title } ... on ScreenOpenChatAction { chatId } ... on ScreenOpenExternalAction { url } ... on ScreenOpenProfileAction { userId } } " +
        "visible { __typename ... on ScreenCondition { comparison negated operand { __typename ... on ScreenTextLiteral { text } ... on ScreenStateRef { path } } path } ... on ScreenVisibleAll { conditions { comparison negated path } } ... on ScreenVisibleFlag { flag } } " +
        "... on ScreenButton { label { __typename ... on ScreenStateRef { path } ... on ScreenTextLiteral { text } } disabled { __typename ... on ScreenFlagLiteral { flag } } } " +
        "... on ScreenColumn { gap { __typename ... on ScreenNumberLiteral { number } } alignItems } ... on ScreenHeading { text { __typename ... on ScreenTextLiteral { text } } level } " +
        "... on ScreenSpacer { length { __typename ... on ScreenNumberLiteral { number } } } ... on ScreenSwitch { checked { __typename ... on ScreenStateRef { path } } label { __typename ... on ScreenTextLiteral { text } } } } " +
        "state { path value { __typename ... on ScreenTextLiteral { text } ... on ScreenNumberLiteral { number } } } } } } }"

    private val firstElements = """
        [
          {"__typename":"ScreenColumn","id":"root","children":["b1","h1","p1","sw1","sp1","f1"],"press":null,"visible":{"__typename":"ScreenVisibleAll","conditions":[{"comparison":"EQ","negated":false,"operand":{"__typename":"ScreenTextLiteral","text":"x"},"path":"user.name"}]},"gap":{"__typename":"ScreenNumberLiteral","number":8},"alignItems":"center"},
          {"__typename":"ScreenButton","id":"b1","children":[],"press":{"__typename":"ScreenNavigateAction","slug":"home","title":"Home"},"visible":null,"label":{"__typename":"ScreenStateRef","path":"user.name"},"disabled":{"__typename":"ScreenFlagLiteral","flag":true}},
          {"__typename":"ScreenHeading","id":"h1","children":[],"press":null,"visible":{"__typename":"ScreenVisibleFlag","flag":true},"text":{"__typename":"ScreenTextLiteral","text":"Hi"},"level":2},
          {"__typename":"ScreenPressable","id":"p1","children":["h1"],"press":{"__typename":"ScreenOpenChatAction","chatId":"c9"},"visible":{"__typename":"ScreenCondition","comparison":"NE","negated":true,"operand":{"__typename":"ScreenStateRef","path":"flag"},"path":"a.b"}},
          {"__typename":"ScreenSwitch","id":"sw1","children":[],"press":null,"visible":null,"checked":{"__typename":"ScreenStateRef","path":"on"},"label":null},
          {"__typename":"ScreenSpacer","id":"sp1","children":[],"press":null,"visible":null,"length":{"__typename":"ScreenNumberLiteral","number":4.5}},
          {"__typename":"ScreenFuture","id":"f1","children":["x"],"size":3}
        ]
    """.trimIndent()

    private fun reply(elements: String, state: String = """[{"path":"user.name","value":{"__typename":"ScreenTextLiteral","text":"Ada"}},{"path":"n","value":{"__typename":"ScreenNumberLiteral","number":3}}]""") =
        """{"data":{"node":{"__typename":"CustomScreen","id":"s1","slug":"main","spec":{"root":"root","elements":$elements,"state":$state}}}}"""

    private suspend fun openRig(): MockTransport {
        val transport = MockTransport()
        DBlayer.database = Database.open(Config(null, null, 64u), modelsJSON, transport)
        DBlayer.database.registerOperations(JsonArray(listOf(buildJsonObject { put("name", JsonPrimitive("ScreenNode")); put("document", JsonPrimitive(document)) })).toString())
        return transport
    }

    @Test
    fun aServerAnswerCarriesEveryElementVariantAndALaterAnswerReplacesTheList() = bounded {
        val transport = openRig()
        transport.enqueue("ScreenNode", reply(firstElements))
        val spec = CustomScreen.node("s1")!!.spec!!
        assertEquals("root", spec.root)
        assertEquals(listOf("root", "b1", "h1", "p1", "sw1", "sp1", null), spec.elements.map { it.id })
        assertEquals(
            listOf(ScreenElementKind.Column, ScreenElementKind.Button, ScreenElementKind.Heading, ScreenElementKind.Pressable, ScreenElementKind.Switch, ScreenElementKind.Spacer, null),
            spec.elements.map { it.kind },
        )
        assertEquals(listOf("b1", "h1", "p1", "sw1", "sp1", "f1"), spec.elements[0].children)
        assertEquals(listOf("h1"), spec.elements[3].children)
        assertEquals(ScreenElement.Unknown("ScreenFuture"), spec.elements[6])
        val button = (spec.elements[1] as ScreenElement.Button).value
        assertEquals(ScreenAction.Navigate(ScreenNavigateAction(slug = "home", title = "Home")), button.press)
        assertEquals(ScreenTextValue.StateRef(ScreenStateRef(path = "user.name")), button.label)
        assertEquals(ScreenFlagValue.FlagLiteral(ScreenFlagLiteral(flag = true)), button.disabled)
        val column = (spec.elements[0] as ScreenElement.Column).value
        assertEquals(ScreenNumberValue.NumberLiteral(ScreenNumberLiteral(number = 8.0)), column.gap)
        assertEquals("center", column.alignItems)
        assertEquals(
            ScreenVisibility.VisibleAll(ScreenVisibleAll(conditions = listOf(ScreenCondition(comparison = "EQ", negated = false, operand = ScreenOperand.TextLiteral(ScreenTextLiteral(text = "x")), path = "user.name")))),
            column.visible,
        )
        assertEquals(ScreenAction.OpenChat(ScreenOpenChatAction(chatId = "c9")), spec.elements[3].press)
        assertEquals(
            ScreenVisibility.Condition(ScreenCondition(comparison = "NE", negated = true, operand = ScreenOperand.StateRef(ScreenStateRef(path = "flag")), path = "a.b")),
            spec.elements[3].visible,
        )
        assertEquals(ScreenVisibility.VisibleFlag(ScreenVisibleFlag(flag = true)), spec.elements[2].visible)
        assertEquals(
            listOf(
                ScreenStateEntry(path = "user.name", value = ScreenStateValue.Text(ScreenTextLiteral(text = "Ada"))),
                ScreenStateEntry(path = "n", value = ScreenStateValue.Number(ScreenNumberLiteral(number = 3.0))),
            ),
            spec.state,
        )

        val second = """
            [
              {"__typename":"ScreenHeading","id":"b1","children":["q"],"press":{"__typename":"ScreenOpenExternalAction","url":"https://x"},"visible":null,"text":{"__typename":"ScreenTextLiteral","text":"now a heading"},"level":null},
              {"__typename":"ScreenColumn","id":"root","children":["b1"],"press":null,"visible":null,"gap":null,"alignItems":null}
            ]
        """.trimIndent()
        transport.enqueue("ScreenNode", reply(second, "[]"))
        CustomScreen.node("s1", force = true)
        val again = CustomScreen.find("s1").current()!!.spec!!
        assertEquals(listOf("b1", "root"), again.elements.map { it.id })
        assertEquals(listOf(ScreenElementKind.Heading, ScreenElementKind.Column), again.elements.map { it.kind })
        assertEquals(listOf("q"), again.elements[0].children)
        assertEquals(ScreenAction.OpenExternal(ScreenOpenExternalAction(url = "https://x")), again.elements[0].press)
        assertNull(again.elements[1].visible)
        assertTrue(again.state.isEmpty())
    }

    @Test
    fun aScreenWrittenWithTypedElementsReadsBackAndAnElementCanChangeItsVariant() = bounded {
        openRig()
        val button = ScreenElement.Button(ScreenButton(id = "b1", children = emptyList(), press = ScreenAction.OpenProfile(ScreenOpenProfileAction(userId = "u1")), label = ScreenTextValue.TextLiteral(ScreenTextLiteral(text = "Go"))))
        val pressable = ScreenElement.Pressable(ScreenPressable(id = "p1", children = listOf("b1")))
        val elements = listOf(button, pressable, ScreenElement.Unknown("ScreenFuture"))
        val created = CustomScreen.create(slug = "local", spec = ScreenSpec(root = "b1", elements = elements, state = emptyList()))
        assertEquals(elements, created.spec!!.elements)

        val spacer = ScreenElement.Spacer(ScreenSpacer(id = "b1", children = emptyList(), length = ScreenNumberValue.NumberLiteral(ScreenNumberLiteral(number = 2.0))))
        val changed = created.update(spec = ScreenSpec(root = "b1", elements = listOf(spacer), state = emptyList()))
        assertEquals(listOf(spacer), changed.spec!!.elements)
        assertEquals(listOf(spacer), CustomScreen.find(created.id).current()!!.spec!!.elements)
    }
}
