# Parent fields copied from children

The parent fields named by `sets` are derived from the server value of the parent row and the stored children, once per commit. A child with a null field leaves the field to the other side: a parent field is null only when no side has a value.
These rules also apply when a partial response creates the child locally for the first time. No extra write from the app is needed.
Deleting the newest child derives the fields from the children left and the server value.
If no child and no server value remain, all copied fields become null.

Read the parent entity after declaring `sets` in the child model:

```swift
let group = Projected<Group>(try scope.entity(Group.self, groupId))
let lastNoteAt = group.value?.lastNoteAt
```

```kotlin
val group = Projected(scope.entity(Group, groupId), Group, screenScope)
val lastNoteAt = group.value?.lastNoteAt
```
