# App migration

## Предзагрузка и участники

Производная предзагрузка: `preload vibeId: { from: "Compass.rows" }, limit: 40`.
`limit` обязателен для `from`, положительный, общий для произведения параметров. Порядок параметров соответствует декларации, id - исходному scope.
Удалить объявления `preload ... { from: ... }` без `limit`. `limit` теперь служебный ключ строки `preload`.
Scope полного состава: `scope :chat_members, members_complete: true do`, существующие `query`, `response`, `paginate size: 100, direction: :forward`.
`max_pages` несовместим с `members_complete`. Серверный предел страницы участников - 100.
Swift: `LiveWindow(try User.chatMembers(chatId).window(tail: 50, max: 200))`.
Kotlin: `LiveWindow(User.chatMembers(chatId).window(tail = 50, max = 200), scope)`.
Читать `snapshot.phase`, `snapshot.error`, `snapshot.complete`, в Kotlin - через `snapshot.value`.
Полнота наступает только после `hasNextPage == false`. Короткая или пустая промежуточная страница не завершает цепочку.
При ошибке частичные строки сохраняются: `Loaded` с `error`, `complete = false`. Без строк - `Failed`.
Последний читатель отменяет запрос. Следующий продолжает сохранённый курсор. Возврат сети продолжает наблюдаемую упавшую цепочку.
Удалить `CompassRowLoads`, ручной `loadAll`, флаг полноты `ChatGroupDetails`, второй список и нарезку строк приложения.
Оставить `expand(before: 40)` / `expand(before = 40)`. Полный состав хранит SQLite, UI получает максимум 200 строк.
`rows`, `failure` и edges `LiveWindow` читают тот же снимок. Перегенерировать модели обеих платформ.

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
Regenerate both platforms and replace the `events` property with `try await Model.events()` on Swift or `Model.events()` on Kotlin.
Start actions after this call returns: the first reader and server subscription are ready.
Remove offline retry loops around this call. The library waits for connectivity and owns cancellation and reader release.
Reusing the returned stream creates an independent reader for each later collection, including after completion or cancellation.
Handle opening errors with their original `DbError` kind. Later collection failures use `ModelEventItem.failed` or `ModelEventItem.Failed`.

## Node membership

Add `membership list: { contains: :lists }` to Chat.list and include `:node` in `insert_on: [:event, :response, :node]`.
Select `viewerState.lists` into Chat.lists in the node response. Chat.status alone does not describe all memberships.
Remove app-side insertion and list refetch after MESSAGE. Regenerate Swift and Kotlin models.
The existing generated list and node calls keep their signatures.
Missing members join the head. Existing members keep their places. Manual node reads do not insert rows into lists.
