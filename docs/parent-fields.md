# Parent fields copied from children

The parent fields named by `sets` are derived fields: declare each one `derived: true` in the parent. The stored children are their only source, and a write of the parent row never changes them.
A response, an event, a local `create` or an `update` of the parent that names such a field keeps the stored value. A new parent row starts with null until the children set it, in the same commit.
The fields come from the newest visible child, once per commit, and are null when no child remains. A child with a null field gives null.
These rules also apply when a partial response creates the child locally for the first time. No extra write from the app is needed.
A child object inside the answer for the parent is written as a row of the child, and the foreign key the answer names is not read.

Read the parent entity after declaring `sets` in the child model:

```swift
let group = Projected<Group>(try scope.entity(Group.self, groupId))
let lastNoteAt = group.value?.lastNoteAt
```

```kotlin
val group = Projected(scope.entity(Group, groupId), Group, screenScope)
val lastNoteAt = group.value?.lastNoteAt
```
