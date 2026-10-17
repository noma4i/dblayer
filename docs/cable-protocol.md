# Cable protocol

`transport::CableLink` implements the cryptographic exchange for `/api/v3/cable`. It has no socket, timer, database or credential storage access.

| Call | Result |
| --- | --- |
| `CableLink::open(config, challenge, credential)` | Connection state and RSA-OAEP/A256GCM hello |
| `seal(body)` | Client frame with the next sequence number |
| `receive(token)` | Authenticated server message with validated fields |
| `close()` | Erases both directional keys and refuses further frames |

Use the HTTP `EncryptedConfig` for cable. The bundled SPKI must match its SHA-256 pin, and the challenge must name that same pin.
The hello binds the audience, path, challenge, credential, client X25519 public key and random response key.
HKDF-SHA256 derives separate client and server keys from the X25519 secret and response key.
The first server frame must be `ready` with the challenge digest. Repeated ready messages, reordered frames and unknown fields are refused.

Frames use compact JWE with the exact protected header `{"alg":"dir","enc":"A256GCM"}`.
Client frames are limited to 32 KiB. Server frames are limited to 256 KiB. Each direction permits 2^20 frames per connection.
An incoming failure retires both keys. An authenticated `closing` message returns its code and reason, then retires both keys.
`CableError` distinguishes untrusted pins, invalid challenges, invalid frames, sequence failures, size limits, exhausted keys and closed connections.

X25519 private material, derived keys and temporary hello plaintext use zeroing storage. Dropping a connection also erases its stored keys.
The public vectors in `fixtures/v3-cable/kat.json` contain test keys only. Tests compare hello plaintext and all four encrypted frames without printing key material.

Swift and Kotlin retain the same `EncryptedConfig(origin, serverKey, pin)` and `openEncrypted` calls.
Платформа передаёт текст WebSocket через `openCable`, `cableSend`, `cableNext`, `cableClose`. Шифрование, подписки, повторные открытия и таймеры выполняет Rust.

## Подписки

`EncryptedTransport.subscribe` использует общий `Transport.subscribe` и `SubscriptionEvent`, как обычный транспорт ActionCable.
Открытие проходит через challenge, подтверждение канала, hello и зашифрованный ready.
Каждая подписка получает отдельный строковый id. Подтверждение `subscribed` выдаёт `Ready`, `next` выдаёт ответ GraphQL.
Отмена отправляет `unsubscribe` только для выбранного id. Последняя отмена закрывает сокет.

| Подписка | Обработка |
| --- | --- |
| `viewerChanges` | `area` и `entityId` выбирают HTTP reread из `changed_by` |
| `viewerNotice` | Событие создаёт строку memory-модели Notice без HTTP-чтения |
| `chatActivity` | `TYPING` обновляет ephemeral `typing` пользователя, `reset_after` сбрасывает значение |
| `interactionReceived` | Событие создаёт строку memory-модели InteractionNotice без HTTP-чтения |

`viewerChanges` не заменяет поля строки. HTTP reread проходит через существующий V3 JWE клиент и обычную запись колонок SQLite.
Уведомления memory-моделей получают UUID при отсутствии серверного id. Их строки существуют только в памяти и удаляются по `expire_after`.
Локальный scope с `updates` открывает подписку при первом читателе и освобождает после последнего с учётом `live_linger`.
События без читателей теряются, сервер их не повторяет.
Необъявленные области увеличивают `SubscriptionStatus.rejected`. Повторный `Ready` перечитывает наблюдаемые выборки моделей с `changed_by`.
Область REACTION может указывать на несколько node-чтений. Тогда выполняется 1 HTTP-запрос на каждую объявленную модель.

Пока есть живая подписка, отсутствие трафика 25 секунд запускает зашифрованный ping.
Если pong или next не приходит за 20 секунд, клиент закрывает сокет и повторяет открытие.
Без подписок отсутствуют соединения и таймеры. Неудачные открытия увеличивают `pacing::Backoff`, успешный ready сбрасывает счётчик.
Офлайн и пустой список подписок не расходуют попытки и не создают задержку повтора, включая отмену во время handshake.
`Sync.set_online` передаёт состояние соединения транспорту.

| Закрытие | Результат |
| --- | --- |
| 4401 | `SessionCode::Unauthenticated`, завершение подписок |
| 4403 или ACCOUNT_LOCKED | `SessionCode::AccountUnavailable`, завершение подписок |
| LOGGED_OUT | `SessionCode::SessionEnded`, завершение подписок |
| 4400, 4408, 4409, 4429, 4430, 4500, 4503 | Повторное открытие с backoff |

Ни одно закрытие не удаляет credential. Cable получает только функцию чтения credential.
`SubscriptionFailure.close` сохраняет код закрытия. Ошибки GraphQL сохраняют сообщения и `GraphqlExtensions` в общем типе ошибки.
Swift `LiveChange.error`, `SubscriptionSink.failed` и `GraphQLSubscriptionEvent.errors` принимают `DbError`, Kotlin использует `DbException`.

Фикстуры `fixtures/v3-cable/subscriptions.json` содержат запросы и ответы всех 4 подписок.
`fixtures/v3-cable/changes.dbl` показывает выбор HTTP-чтений. Фейковый сокет проверяет полный обмен, HTTP JWE и запись строки.
