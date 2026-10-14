# Nested value types

Start with the [model syntax and decode rules](models.md#nested-objects-and-lists). Use blocks for known JSON fields and unrestricted JSON for values whose structure changes.

| Declaration | Result |
| ----------- | ------ |
| `object :settings do ... end` in `Message` | generated `MessageSettings` |
| `array :items, as: GiftItem do ... end` | `[GiftItem]` in Swift, `List<GiftItem>` in Kotlin |
| `object :gift, as: GiftItem` without a block | reuse a type declared anywhere in the same generator invocation |
| `object`, `array` without a block or `as:` | unrestricted JSON values |
| `raw :value` | any JSON value, including mixed objects and arrays |
| `string :name, from: "full_name"` inside a block | property `name`, JSON key `full_name` |
| `list :owners, of: :id` inside a block | string IDs, including numeric input |

## Defaults

Defaults accept scalar values, non-empty lists, JSON values and typed objects. A typed object default must supply its required fields. A default can accompany `null` or `optional`.

Inside a model's `schema`:

```ruby
object :settings do
  list :labels, of: :string, default: ["new"]
  string :theme, null: true, default: "system"
  raw :extras, default: { flags: [true, false] }
  object :offset, default: { x: 0, y: 0 } do
    num :x
    num :y
  end
end
```

The generated constructors and decoders use these values. A nullable field reads its default when absent and remains null when explicitly null. A required field with a default uses it for absent or null input. A value with the wrong type gives a decoding error.

## Reuse and recursion

`as` names a generated type. Define it once and reuse it across fields, lists and models. File order does not matter. Pass the declaring files and the referencing files together to the generator.

Recursive values can use arrays:

```ruby
object :tree, as: Tree do
  string :name
  array :children, as: Tree, default: []
end
```

An object cycle without an array cannot form a Swift struct and is rejected. Array recursion works on both platforms. Nested values do not become rows or gain independent subscriptions.

## ID conversion

Generated Swift uses `IDText` for numeric IDs, and generated Kotlin uses the `IDText` and `IDTexts` serializers. Use the generated model types in apps. The library owns these conversion types and accepts JSON only.
