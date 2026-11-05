# api_gap: тесты фазы T против API из спеки

Тесты в `outbox_api_tests.rs` и `transfer_api_tests.rs` под `cfg(feature = "spec_api")`. Не компилируются на текущем коде, до фазы C не запускаются.

| Тест | Чего нет в API | Предложение формы |
| --- | --- | --- |
| out_submitting_rejected | отказ коммита Store по заданной причине | `Database::reject_next_commit_for_testing(StoreRejection::{StaleGeneration, NoSession, Storage})`, `DbError::Rejected{reason}` для вызова автора |
| out_queued_discard, out_failed_discard | исход `Discarded` (сейчас `Interrupted`) | `WireOperationOutcome::Discarded` |
| out_sending_reject_keep | исход `Failed{error}` (сейчас `Rejected`) | `WireOperationOutcome::Failed{failure}` |
| out_queued_models_reset, out_landing_stale_generation, out_rollback_stale_generation, out_prop_confirmed_lands | вход `ModelsReset(models)` из тестов | `Database::reset_models_for_testing(Vec<String>)` |
| out_sending_discard | ошибка `InFlight` | `DbError::InFlight{detail}` |
| out_landing_no_session, out_rollback_no_session, out_landing_storage, out_rollback_stale_generation | отказ коммита посадки или отката по причине | то же `reject_next_commit_for_testing` |
| out_epoch_ended, out_outcome_stale, out_prop_no_send_after_end, xfer_epoch_ended, xfer_round_*_epoch/new_epoch/end_next, xfer_prop_epoch | `EpochEnded(cause)` и `EpochStarted` без encrypted-сервера | `Database::end_session_for_testing(EpochCause)` и `start_session_for_testing(identity)` на обычном транспорте |
| out_outcome_stale | `Err(NoSession)` для квитанции прошлой эпохи (сейчас `Interrupted`) | вызов `outcome` проверяет эпоху квитанции |
| out_submit_stale, xfer_accept_stale | (текущий API компилируется) вызов с прошлой эпохой даёт `Interrupted`, спека требует `NoSession`: красный на коде | - |
| out_landing_committed, out_prop_confirmed_lands | `Confirmed{revision}` | `WireOperationOutcome::Confirmed{revision, ..}`; тест на текущем API без ревизии |
| out_prop_membership_revision | ответ списка с `base_revision`, членство списка в снимке | `Server::script_list_answer`, `Rig::member_of_list` над публичным чтением окна |
| out_queued_send | вход `EpochStarted` как причина отправки | `start_session_for_testing` |
| out_prop_idle, xfer_prop_idle | счётчик пробуждений (Tick) | `Database::wake_count()` |
| xfer_copy_done, xfer_copy_failed, xfer_copy_cancel, xfer_accept | управляемое копирование (состояние `Copying` видно только при удержании) | `hold_copy_for_testing`, `release_copy_for_testing`, `fail_copy_for_testing`, `copy_cancelled_for_testing` |
| xfer_prop_purpose | `NetworkPurpose::Upload` (сейчас `Transfer`) | переименовать вариант или спека использует `Transfer` |
| xfer_round_needed_running, xfer_round_needed_new_epoch, xfer_round_end_next, xfer_round_epoch_ended | `RoundNeeded` второй эпохи, `Requested{next}`, ожидание `RoundEnded` с эпохой раунда | `round_end(epoch, owner)` возвращает позднюю группу `next`; см. `end_session_for_testing` |
| xfer_round_refused | (текущий API) `background_refused` ставит ошибку на принятую операцию | - |
| проекция | `Failed`, `Discarded` в трассах `out_prop_*` сравниваются как `Rejected`, `Interrupted` | исчезает вместе с вариантами `WireOperationOutcome` выше |
