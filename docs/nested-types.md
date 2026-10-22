# Nested value types

Start with the [model syntax and decode rules](models.md#nested-objects-and-lists). Use blocks for known JSON fields and unrestricted JSON for values whose structure changes.

| Declaration | Result |
| ----------- | ------ |
| `field :settings, type: :object do ... end` in `Note` | generated `NoteSettings` |
| `field :items, type: [RewardItem] do ... end` | `[RewardItem]` in Swift, `List<RewardItem>` in Kotlin |
| `field :reward, type: RewardItem` without a block | reuse a type declared anywhere in the same generator invocation |
| `type: :object`, `type: :array` without a block | unrestricted JSON values |
| `field :value, type: :raw` | any JSON value, including mixed objects and arrays |
| `field :name, type: :string, from: "full_name"` inside a block | property `name`, server response path and canonical key `full_name` |
| `field :owners, type: [:id]` inside a block | string IDs, including numeric input |
| `field :attachment, type: NoteAttachment do on RewardAttachment do ... end ... end` | a union, see [unions](#unions) |
| `field :elements, type: [SampleElement] do shared do ... end on Button do ... end end` | a list of interface values, see [interfaces and lists](#interfaces-and-lists-of-unions) |

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

## JSON text in raw fields

Declare `field :payload, type: :raw, parse: :json, null: true` when the response sends JSON inside a string.
Core parses the string once and stores its JSON tree, including nested objects and arrays.
Invalid JSON text and non-string input fail value decoding, nullable null remains null.
Defaults must be objects or null, string defaults are rejected and never parsed.
Ephemeral and local writes use the same string input rule, their stored values and defaults remain decoded JSON.
Edge fields reject `parse`, their storage accepts only scalar values.
Stored values survive later merges and writes without being parsed again.
For `{"payload":"{\"items\":[1,{\"enabled\":true}]}"}`, both platforms receive the object directly.

```swift
let items = sample.payload?.objectValue["items"]
```

```kotlin
val items = sample.payload?.jsonObject?.get("items")
```

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
model Note do
  field :attachment, type: NoteAttachment, null: true do
    on RewardAttachment do
      field :id, type: :id
      field :tier, type: :string, from: "rewardTier"
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
case .reward(let reward): show(reward.tier)
case .moment(let moment): open(moment.momentId)
case .reaction(let reaction): show(reaction.body)
case .unknown(let typename): log(typename)
case nil: break
}
let rewards = Note.where(Note.attachment == .reward)
```

```kotlin
when (val attachment = message.attachment) {
    is NoteAttachment.Reward -> show(attachment.value.tier)
    is NoteAttachment.Moment -> open(attachment.value.momentId)
    is NoteAttachment.Reaction -> show(attachment.value.body)
    is NoteAttachment.Unknown -> log(attachment.typename)
    null -> Unit
}
val rewards = Note.where(Note.attachment eq NoteAttachmentKind.Reward)
```

| Rule | Behavior |
| ---- | -------- |
| Case names | the variant type name without the words that every variant starts or ends with: `RewardAttachment` becomes `reward`, `MomentEmojiProps` becomes `emoji`. `on Type, case: :name` sets it |
| Reuse | `type: NoteAttachment` without a block names a union declared anywhere in the same generator invocation. A variant type can belong to several unions |
| `null` | a nullable union field is `nil` / `null` when the server sends no value. An absent value is not an unknown variant |
| Unknown variant | a `__typename` that no `on` block names becomes `.unknown(typename:)` / `Unknown`. It keeps only the type name: its fields are not stored and are not written back |
| Partial selections | an operation may select some fields of a variant. Declare the others with `null: true` or `optional: true` |
| Answers | an answer replaces the whole value, like any nested value. A new variant clears the columns of the old one |
| Merge | `merge: :present` keeps the stored value when the server sends null, and `local: true` keeps the field app-owned. Key-by-key merge is not available for a union |
| Queries | `Note.attachment` compares the variant kind: `==`, `in`, `isNull`, ordering. Variant fields are not queryable |
| Lists | `type: [Name]` with `on` blocks holds a list of variants, see [interfaces and lists](#interfaces-and-lists-of-unions) |
| Schema | with `--schema`, every `on` type must be a member of the GraphQL union and every member needs an `on` block. An operation document that selects the field must select `__typename` and may name only declared variants in `... on` |

A union is stored in the columns of its row: one text column holds the `__typename`, and each variant has its own columns, prefixed with the field and variant names. Only the active variant has values. A SQLite table has at most 2000 columns, so the generator rejects a model whose union widens a table past that limit: `table row_Note would have 2405 columns, above the SQLite limit of 2000`.

## Interfaces and lists of unions

A GraphQL interface is a union with a `shared` block. The `shared` block declares the interface fields once. Every `on` block declares only the fields of its implementation. A field type of `[Name]` with `on` blocks is a list of variants: each item is one variant.

```ruby
model InterfaceRecord do
  field :slug, type: :string
  field :spec, type: SampleSpec, null: true do
    field :root, type: :id
    field :elements, type: [SampleElement] do
      shared do
        field :id, type: :id
        field :children, type: [:id]
        field :press, type: SampleAction, null: true
        field :visible, type: SampleVisibility, null: true
      end
      on SampleButton do
        field :label, type: SampleTextValue, null: true
      end
      on SampleHeading do
        field :text, type: SampleTextValue, null: true
      end
      on SamplePressable do
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
        is SampleElement.Button -> show(id, element.value.label)
        is SampleElement.Heading -> show(id, element.value.text)
        is SampleElement.Pressable, is SampleElement.Unknown -> Unit
    }
    open(element.press, element.children.orEmpty())
}
```

| Rule | Behavior |
| ---- | -------- |
| Shared fields | `shared` is allowed once in a union field. A shared field and a variant field cannot have the same name. `kind` is reserved when a union has shared fields |
| Empty variant | `on Type do end` is allowed when the union has shared fields: the variant holds only them |
| Reuse | `type: [SampleElement]` or `type: SampleElement` without a block names an interface declared anywhere in the same generator invocation. A union or interface field may be nullable |
| Tree | a field such as `children` holds keys (`[:id]`), not nested values. The tree is not stored recursively: a screen reads children by key from the same list |
| Order | the list keeps the order the server sent. An answer replaces the whole list, and an element that changes variant loses the columns and child rows of the old variant |
| Unknown variant | an item with an unknown `__typename` becomes `.unknown(typename:)` / `Unknown`. It keeps the type name only, so its shared fields are empty. An item without a text `__typename` is dropped |
| Nested unions | a variant or the `shared` block may hold unions and lists of values. A variant of one union may hold a union, and a type may be a variant of several unions |
| Queries | list items are not queryable. A single interface value compares its kind like any union |
| Schema | with `--schema`, the `on` types must be the implementations of the interface and every implementation needs an `on` block. The `shared` fields must exist on the interface and on every implementation. The operation must select `__typename` and may name only declared variants in `... on` |

An interface list is stored in one child table with one row per item: one text column holds the `__typename`, the shared fields take their columns once, and each variant adds its own columns. A model of 33 variants with 334 columns of their own and 20 shared columns takes 359 columns per item. A table has at most 2000 columns, so the generator rejects a wider declaration and names the table: `table row_Sample__spec would have 2406 columns, above the SQLite limit of 2000`. Declare fewer variants or fields, or split the model.

## Wide interface lists

Count 4 storage columns, 1 discriminator, shared columns once, and the sum of every variant's columns.
A scalar costs 1 column, a four-case scalar value costs 5, and a list costs 1 presence column plus its child table.
The generator rejects tables above 2000 columns.
Use a single element type with a string `type` and nullable `raw` props when the property catalogue changes independently.
In `fixtures/screen.dbl`, 14 common fields plus 1 raw props field and 4 storage columns make 19 columns per element.
Adding catalogue types or props keeps that width constant.
Props, visibility, action lists and state values use `raw, parse: :json`, so the application receives decoded JSON values.
Applications interpret props and conditions after core parses their JSON text.
The state entry table has 4 storage columns, 1 path and 1 raw value, totaling 6 columns instead of 9.
Action lists and conditions no longer declare separate variant tables or generated types.

The groups below define a fixed-shape catalogue example, with 20 base entries in group 0 and 20 component groups.
The example has 306 listed components, including 102 composable components.
Only composable rows declare V/S/L widths, the other rows have no typed properties to count.

| Group | Listed | Composable | V | S | L | 5V + S |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 20 | 20 | 17 | 36 | 1 | 121 |
| 1 | 47 | 7 | 9 | 2 | 0 | 47 |
| 2 | 50 | 15 | 26 | 13 | 1 | 143 |
| 3 | 7 | 2 | 3 | 0 | 0 | 15 |
| 4 | 5 | 5 | 8 | 6 | 1 | 46 |
| 5 | 10 | 1 | 0 | 3 | 0 | 3 |
| 6 | 24 | 3 | 4 | 1 | 2 | 21 |
| 7 | 13 | 0 | 0 | 0 | 0 | 0 |
| 8 | 3 | 0 | 0 | 0 | 0 | 0 |
| 9 | 7 | 2 | 3 | 0 | 0 | 15 |
| 10 | 16 | 7 | 15 | 5 | 0 | 80 |
| 11 | 31 | 11 | 9 | 6 | 2 | 51 |
| 12 | 6 | 4 | 4 | 2 | 1 | 22 |
| 13 | 16 | 8 | 14 | 5 | 1 | 75 |
| 14 | 4 | 1 | 1 | 2 | 0 | 7 |
| 15 | 8 | 4 | 5 | 2 | 0 | 27 |
| 16 | 13 | 6 | 11 | 2 | 1 | 57 |
| 17 | 3 | 0 | 0 | 0 | 0 | 0 |
| 18 | 30 | 19 | 23 | 16 | 1 | 131 |
| 19 | 5 | 4 | 7 | 4 | 0 | 39 |
| 20 | 8 | 3 | 5 | 3 | 0 | 28 |
| Components | 306 | 102 | 147 | 72 | 10 | 807 |
| Including group 0 | 326 | 122 | 164 | 108 | 11 | 928 |

The typed element table totals 4 + 1 + 13 shared + 928 variant columns + 11 list presence columns = 957.
Both declarations fit today, a synthetic threefold expansion reaches 2835 columns and must fail.
Raw props also preserve fields of unknown element types, while an unknown union variant preserves only its discriminator.
