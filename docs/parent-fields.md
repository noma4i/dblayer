# Parent fields copied from children

For an upsert, `sets` copies only fields present in the merged child row. An omitted field preserves the parent value. Explicit null clears it.
These rules also apply when a partial response creates the child locally for the first time. No extra write from the app is needed.
Deleting the current child selects another child and replaces the copied fields, clearing fields that the replacement does not have.
If no child remains, all copied fields become null.

Read the parent entity after declaring `sets` in the child model:

```swift
let group = Projected<Group>(try scope.entity(Group.self, groupId))
let lastNoteAt = group.value?.lastNoteAt
```

```kotlin
val group = Projected(scope.entity(Group, groupId), Group, screenScope)
val lastNoteAt = group.value.value?.lastNoteAt
```
