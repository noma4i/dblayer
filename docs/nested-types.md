# Nested value types

Start with the [model syntax and decode rules](models.md#nested-objects-and-lists). Use blocks for known JSON fields and unrestricted JSON for values whose structure changes.

| Declaration | Result |
| ----------- | ------ |
| `field :settings, type: :object do ... end` in `Message` | generated `MessageSettings` |
| `field :items, type: [GiftItem] do ... end` | `[GiftItem]` in Swift, `List<GiftItem>` in Kotlin |
| `field :gift, type: GiftItem` without a block | reuse a type declared anywhere in the same generator invocation |
| `type: :object`, `type: :array` without a block | unrestricted JSON values |
| `field :value, type: :raw` | any JSON value, including mixed objects and arrays |
| `field :name, type: :string, from: "full_name"` inside a block | property `name`, server response path and canonical key `full_name` |
| `field :owners, type: [:id]` inside a block | string IDs, including numeric input |
| `field :attachment, type: MessageAttachment do on GiftAttachment do ... end ... end` | a union, see [unions](#unions) |
| `field :elements, type: [ScreenElement] do shared do ... end on Button do ... end end` | a list of interface values, see [interfaces and lists](#interfaces-and-lists-of-unions) |

## Defaults

Defaults accept scalar values, non-empty lists, JSON values and typed objects. A typed object default must supply its required fields. A default can accompany `null` or `optional`.

Inside a model:

```ruby
field :settings, type: :object do
  field :labels, type: [:string], default: ["new"]
  field :theme, type: :string, null: true, default: "system"
  field :extras, type: :raw, default: { flags: [true, false] }
  field :offset, type: :object, default: { x: 0, y: 0 } do
    field :x, type: :num
    field :y, type: :num
  end
end
```

The generated constructors and decoders use these values. A nullable field reads its default when absent and remains null when explicitly null. A required field with a default uses it for absent or null input. A value with the wrong type gives a decoding error.

## Reuse and recursion

`type: NamedType` names a generated value type. Define it once and reuse it across fields, lists and models. File order does not matter. Pass the declaring files and the referencing files together to the generator.

Recursive values can use arrays:

```ruby
field :tree, type: Tree do
  field :name, type: :string
  field :children, type: [Tree], default: []
end
```

An object cycle without an array cannot form a Swift struct and is rejected. Array recursion works on both platforms. Nested values do not become rows or gain independent subscriptions.

## ID conversion

Generated Swift uses `IDText` for numeric IDs, and generated Kotlin uses the `IDText` and `IDTexts` serializers. Use the generated model types in apps. The library owns these conversion types. They apply to JSON answers. A model reads ids from cells and turns a numeric id into text the same way.

## Storage

A list, a typed object and an array of a typed value are stored in SQLite columns and child tables, not as JSON text. A typed object becomes columns of its row, a list or an array becomes one child table with a position per item, and an array inside an array uses the same table with a link to its parent item. Recursive arrays keep every level in one table. A key that the declaration does not name is dropped when the row is stored, and so is a value of the wrong type. A field with a free shape (`raw`, `file`, an object or array without a type, or a typed value that holds one) is stored as a tree of rows in one node table of its model: one row per value with its parent, position, key, kind and the number or text itself. A null stored in such a field and an absent field are the same state. A filter on a free field sees whether it is present and compares scalar values.

## Unions

A GraphQL union is a field with one `on` block per variant. Each block declares an ordinary nested type, named like the GraphQL type, and the field holds the variant the server sent.

```ruby
model Message do
  field :attachment, type: MessageAttachment, null: true do
    on GiftAttachment do
      field :id, type: :id
      field :tier, type: :string, from: "giftTier"
      field :status, type: :string, null: true
    end
    on MomentAttachment do
      field :momentId, type: :id
    end
    on ReactionAttachment do
      field :body, type: :string
    end
  end
end

model MomentWidget do
  field :props, type: MomentWidgetItemProps, null: true do
    on MomentEmojiProps do
      field :emoji, type: :string
    end
    on MomentLabelProps do
      field :text, type: :string
    end
  end
end
```

The generator reads `__typename` of every server value. Swift gets an `enum` and Kotlin a `sealed class`, with one case per variant and an unknown case.

```swift
switch message.attachment {
case .gift(let gift): show(gift.tier)
case .moment(let moment): open(moment.momentId)
case .reaction(let reaction): show(reaction.body)
case .unknown(let typename): log(typename)
case nil: break
}
let gifts = Message.where(Message.attachment == .gift)
```

```kotlin
when (val attachment = message.attachment) {
    is MessageAttachment.Gift -> show(attachment.value.tier)
    is MessageAttachment.Moment -> open(attachment.value.momentId)
    is MessageAttachment.Reaction -> show(attachment.value.body)
    is MessageAttachment.Unknown -> log(attachment.typename)
    null -> Unit
}
val gifts = Message.where(Message.attachment eq MessageAttachmentKind.Gift)
```

| Rule | Behavior |
| ---- | -------- |
| Case names | the variant type name without the words that every variant starts or ends with: `GiftAttachment` becomes `gift`, `MomentEmojiProps` becomes `emoji`. `on Type, case: :name` sets it |
| Reuse | `type: MessageAttachment` without a block names a union declared anywhere in the same generator invocation. A variant type can belong to several unions |
| `null` | a nullable union field is `nil` / `null` when the server sends no value. An absent value is not an unknown variant |
| Unknown variant | a `__typename` that no `on` block names becomes `.unknown(typename:)` / `Unknown`. It keeps only the type name: its fields are not stored and are not written back |
| Partial selections | an operation may select some fields of a variant. Declare the others with `null: true` or `optional: true` |
| Answers | an answer replaces the whole value, like any nested value. A new variant clears the columns of the old one |
| Merge | `merge: :present` keeps the stored value when the server sends null, and `local: true` keeps the field app-owned. Key-by-key merge is not available for a union |
| Queries | `Message.attachment` compares the variant kind: `==`, `in`, `isNull`, ordering. Variant fields are not queryable |
| Lists | `type: [Name]` with `on` blocks holds a list of variants, see [interfaces and lists](#interfaces-and-lists-of-unions) |
| Schema | with `--schema`, every `on` type must be a member of the GraphQL union and every member needs an `on` block. An operation document that selects the field must select `__typename` and may name only declared variants in `... on` |

A union is stored in the columns of its row: one text column holds the `__typename`, and each variant has its own columns, prefixed with the field and variant names. Only the active variant has values. A SQLite table has at most 2000 columns, so the generator rejects a model whose union widens a table past that limit: `table row_Message would have 2405 columns, above the SQLite limit of 2000`.

## Interfaces and lists of unions

A GraphQL interface is a union with a `shared` block. The `shared` block declares the interface fields once. Every `on` block declares only the fields of its implementation. A field type of `[Name]` with `on` blocks is a list of variants: each item is one variant.

```ruby
model CustomScreen do
  field :slug, type: :string
  field :spec, type: ScreenSpec, null: true do
    field :root, type: :id
    field :elements, type: [ScreenElement] do
      shared do
        field :id, type: :id
        field :children, type: [:id]
        field :press, type: ScreenAction, null: true
        field :visible, type: ScreenVisibility, null: true
      end
      on ScreenButton do
        field :label, type: ScreenTextValue, null: true
      end
      on ScreenHeading do
        field :text, type: ScreenTextValue, null: true
      end
      on ScreenPressable do
      end
    end
  end
end
```

Each variant type holds the shared fields first and its own fields after them. The union type gives one property for each shared field. The property is empty for an unknown variant.

```swift
let elements = screen.spec?.elements ?? []
for element in elements {
  guard let id = element.id else { continue }
  switch element {
  case .button(let button): show(id, button.label)
  case .heading(let heading): show(id, heading.text)
  case .pressable, .unknown: break
  }
  open(element.press, children: element.children ?? [])
}
```

```kotlin
val elements = screen.spec?.elements.orEmpty()
for (element in elements) {
    val id = element.id ?: continue
    when (element) {
        is ScreenElement.Button -> show(id, element.value.label)
        is ScreenElement.Heading -> show(id, element.value.text)
        is ScreenElement.Pressable, is ScreenElement.Unknown -> Unit
    }
    open(element.press, element.children.orEmpty())
}
```

| Rule | Behavior |
| ---- | -------- |
| Shared fields | `shared` is allowed once in a union field. A shared field and a variant field cannot have the same name. `kind` is reserved when a union has shared fields |
| Empty variant | `on Type do end` is allowed when the union has shared fields: the variant holds only them |
| Reuse | `type: [ScreenElement]` or `type: ScreenElement` without a block names an interface declared anywhere in the same generator invocation. A union or interface field may be nullable |
| Tree | a field such as `children` holds keys (`[:id]`), not nested values. The tree is not stored recursively: a screen reads children by key from the same list |
| Order | the list keeps the order the server sent. An answer replaces the whole list, and an element that changes variant loses the columns and child rows of the old variant |
| Unknown variant | an item with an unknown `__typename` becomes `.unknown(typename:)` / `Unknown`. It keeps the type name only, so its shared fields are empty. An item without a text `__typename` is dropped |
| Nested unions | a variant or the `shared` block may hold unions and lists of values. A variant of one union may hold a union, and a type may be a variant of several unions |
| Queries | list items are not queryable. A single interface value compares its kind like any union |
| Schema | with `--schema`, the `on` types must be the implementations of the interface and every implementation needs an `on` block. The `shared` fields must exist on the interface and on every implementation. The operation must select `__typename` and may name only declared variants in `... on` |

An interface list is stored in one child table with one row per item: one text column holds the `__typename`, the shared fields take their columns once, and each variant adds its own columns. A model of 33 variants with 334 columns of their own and 20 shared columns takes 359 columns per item. A table has at most 2000 columns, so the generator rejects a wider declaration and names the table: `table row_Screen__spec would have 2406 columns, above the SQLite limit of 2000`. Declare fewer variants or fields, or split the model.
