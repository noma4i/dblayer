# Nested value types

Start with the [model syntax and decode rules](models.md#nested-objects-and-lists). Use blocks for known JSON fields and unrestricted JSON for values whose structure changes.

| Declaration | Result |
| ----------- | ------ |
| `field :settings, type: :object do ... end` in `Message` | generated `MessageSettings` |
| `field :items, type: [GiftItem] do ... end` | `[GiftItem]` in Swift, `List<GiftItem>` in Kotlin |
| `field :gift, type: GiftItem` without a block | reuse a type declared anywhere in the same generator invocation |
| `type: :object`, `type: :array` without a block | unrestricted JSON values |
| `field :value, type: :raw` | any JSON value, including mixed objects and arrays |
| `field :name, type: :string, from: "full_name"` inside a block | property `name`, JSON key `full_name` |
| `field :owners, type: [:id]` inside a block | string IDs, including numeric input |
| `field :attachment, type: MessageAttachment do on GiftAttachment do ... end ... end` | a union, see [unions](#unions) |

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
| Lists | a list of unions is not supported |
| Schema | with `--schema`, every `on` type must be a member of the GraphQL union and every member needs an `on` block. An operation document that selects the field must select `__typename` and may name only declared variants in `... on` |

A union is stored in the columns of its row: one text column holds the `__typename`, and each variant has its own columns, prefixed with the field and variant names. Only the active variant has values. A SQLite table has at most 2000 columns, so the generator rejects a model whose union widens a table past that limit: `table row_Message would have 2405 columns, above the SQLite limit of 2000`.
