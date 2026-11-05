# api_gap: тесты направления B, которым нужен вход или наблюдаемое вне текущего публичного API

Тесты лежат в `*_gap_tests.rs` под `cfg(feature = "spec_api")` (в `core/Cargo.toml`). Крючки названы методами `World` (`core/src/spec/harness.rs`), в фазе C они получают реализацию.

| Тест | Чего нет в API | Предложение формы |
| --- | --- | --- |
| dlv_opening_read_failed, dlv_live_read_failed | вход `ReadFailed`: сбой чтения SQLite | `Database::fail_next_read_for_testing()` |
| dlv_opening_committed, dlv_opening_unrelated, dlv_opening_window, dlv_opening_event, dlv_opening_pull | состояние `Opening` держится только пока идёт 1-е чтение | `Database::hold_reads_for_testing()` и `release_reads_for_testing()` |
| dlv_live_read_old | вход `ReadDone` старой ревизии | через `hold_reads` + обратный порядок выдачи чтений |
| dlv_live_event_gap, dlv_live_event_old, dlv_live_event_rows_kind, dlv_live_lost, dlv_opening_event | входы `ModelEvent{position}`, `ModelLost` в `Delivery` | `Database::inject_model_event_for_testing(stream, position)` и `inject_model_lost_for_testing(stream, first, last)` |
| dlv_epoch_ended, dlv_epoch_other, dlv_prop_epoch, win_epoch_ended, feed_epoch_ended | вход `EpochEnded(e, cause)` на `Database` без шифрованной сессии (в `Database` без `encrypted` сессия не кончается) | `Database::end_epoch_for_testing(cause)` |
| dlv_opening_models_reset, dlv_opening_models_other, dlv_live_models_reset, dlv_live_models_other, win_idle_models_reset, win_idle_models_other, win_loading_models_reset, win_loading_models_other, win_prop_generation, feed_live_models_reset, feed_prop_reset_other_model | вход `ModelsReset(models)` | `Database::reset_models_for_testing(models)` (вызывает `Lifecycle.ResetModels`) |
| win_idle_stale_watched, win_idle_stale_unwatched, win_loading_stale, win_landing_stale, win_landing_committed_again, win_landing_wait | вход `Stale` окну с `revision` новее запроса | `Database::stale_window_for_testing(model, relation)` |
| win_loading_stale_covered, win_landing_wait | вход `Stale` окну с `revision` не новее запроса | `Database::stale_window_covered_for_testing(model, relation)` |
| win_landing_stale_generation, win_landing_no_session, win_landing_storage, feed_live_rejected_generation, feed_live_rejected_storage, feed_live_rejected_session | вход `Rejected(reason)` от Store и удержание посадки в `Landing` | `Database::reject_next_commit_for_testing(reason)`, `hold_commit_for_testing()` и `release_commit_for_testing()` |
| win_landing_join, win_landing_wait | состояние `Landing` держится только пока идёт коммит | `hold_commit_for_testing()` |
| feed_landing_after_drop, feed_stray_batch | ответ Store по батчу после обрыва и ответ по чужому батчу | `hold_commit_for_testing()` (есть выше), `Database::inject_stray_commit_reply_for_testing()` |
| feed_live_event_unwatched | событие модели без читателя и без окна при живой подписке другой модели | фикстура `World::events_stream_of(model)` |
| feed_live_channel_lost | вход `ChannelLost{dropped, models}` (переполнение входного канала транспорта) | `SubscriptionSink::overflow_for_testing(dropped)` |
| feed_opening_want, feed_live_want_more | нужна 2-я модель с `change_feed` в фикстуре и наблюдаемое «набор подписки сменился» | фикстура `World::events_stream_of(model)` + счётчик `subscribe` в моке (есть) |
| dlv_opening_*, все `Opening` | наблюдаемое «ничего» до `ReadDone` без гонки | см. `hold_reads_for_testing` |

Прочие расхождения спеки и текущего API (не пробелы, а красные тесты фазы C) перечислены в отчёте `.orca-artifacts/t-b/report.md`.
