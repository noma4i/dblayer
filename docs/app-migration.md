# App migration

## Create sources

Replace singleton reads and copied create arguments with repeated `local :field, from: { of: Model, field: :source }` declarations.
Regenerate Swift and Kotlin models. Each sourced argument is optional on both platforms.
Keep explicit product values as arguments. Use each destination once.
Remove `from:` from update declarations. Update calls pass their local values explicitly.

## Subscription filters

Replace `matches: { kind: "ACTIVE" }` with `matches: { kind: ["ACTIVE"] }`.
Use `matches: { kind: { not: ["SCREENSHOT"] } }` for exclusion.
Declare ChatAction with the five accepted activity kinds. Declare ChatActivityNotice with `["SCREENSHOT"]` on the same subscription.
Remove app-side kind switches, receive/stop calls and notice copying. Regenerate both platforms.
Model-level subscription declarations expose `Model.events()` on Swift and Kotlin.

## Node membership

Add `membership list: { contains: :lists }` to Chat.list and include `:node` in `insert_on: [:event, :response, :node]`.
Select `viewerState.lists` into Chat.lists in the node response. Chat.status alone does not describe all memberships.
Remove app-side insertion and list refetch after MESSAGE. Regenerate Swift and Kotlin models.
The existing generated list and node calls keep their signatures.
Missing members join the head. Existing members keep their places. Manual node reads do not insert rows into lists.
