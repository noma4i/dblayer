fn apply_transitions(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    generation: Generation,
    transitions: &[OperationTransition],
    now_ms: i64,
    synced: &mut BTreeSet<SyncKey>,
    result: &mut CommitResult,
) -> Result<bool, StoreError> {
    let mut wrote = false;
    for transition in transitions {
        match transition {
            OperationTransition::BeginBlob { record, item, id, name } => {
                let seq = begin_operation(connection, record)?;
                crate::operation::write_upload(connection, seq, 0, &item.state)?;
                write_item_source(connection, seq, 0, item)?;
                connection.execute("INSERT INTO blob_upload (id, name, seq, generation) VALUES (?1, ?2, ?3, ?4)", params![id, name, seq, generation.value() as i64])?;
                result.operations.insert(seq);
                wrote = true;
            }
            OperationTransition::RetireBlob { id, expired } => {
                let row: Option<(Option<i64>, Option<i64>)> = connection.query_row("SELECT seq, consumer_seq FROM blob_upload WHERE id = ?1", [id], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
                if let Some((Some(seq), owner)) = row {
                    if owner.is_some() {
                        if *expired { continue; }
                        return Err(StoreError::Constraint("Blob is pinned to an operation".into()));
                    }
                    crate::operation::record_transfer_aborts(connection, seq, None)?;
                    connection.execute("UPDATE blob_upload SET seq = NULL, ready = 0, expired = ?1 WHERE id = ?2", params![expired, id])?;
                    delete_operation(connection, seq)?;
                    result.operations.insert(seq);
                    wrote = true;
                }
            }
            OperationTransition::BeginUpload { record, items } => {
                let seq = begin_operation(connection, record)?;
                for (position, item) in items.iter().enumerate() {
                    let position = position as i64;
                    crate::operation::write_upload(connection, seq, position, &item.state)?;
                    write_item_source(connection, seq, position, item)?;
                }
                synced.extend(operation_keys(&record.action_key, &record.row_ids, None));
                result.operations.insert(seq);
                wrote = true;
            }
            OperationTransition::ReplaceUploadSource { seq, position, item } => {
                synced.extend(op_keys_of(connection, *seq)?);
                crate::operation::record_transfer_aborts(connection, *seq, Some(*position))?;
                connection.execute("DELETE FROM file WHERE id = (SELECT file_id FROM op_item WHERE seq = ?1 AND position = ?2) AND field LIKE 'op_item.%'", params![seq, position])?;
                connection.execute("DELETE FROM op_part WHERE seq = ?1 AND item_position = ?2", params![seq, position])?;
                connection.execute("DELETE FROM op_item_retry WHERE seq = ?1 AND position = ?2", params![seq, position])?;
                crate::operation::write_upload(connection, *seq, *position, &item.state)?;
                write_item_source(connection, *seq, *position, item)?;
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::RemoveUploadItem { seq, position, inputs } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let key: Option<String> = connection.query_row("SELECT client_key FROM op_item WHERE seq = ?1 AND position = ?2", params![seq, position], |row| row.get(0)).optional()?;
                result.discarded_items.extend(key.map(|key| (*seq, key)));
                super::upload::remove_item(connection, *seq, *position)?;
                for (item_position, input) in inputs {
                    let value = serde_json::to_value(input).map_err(|error| StoreError::Constraint(error.to_string()))?;
                    crate::operation::write_item_value(connection, *seq, *item_position, "input", Some(&value))?;
                }
                let rows = read_rows(connection, *seq)?;
                if rows.is_empty() {
                    delete_operation(connection, *seq)?;
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::ConfirmUploadItem { seq, position, item, temp, row } => {
                synced.extend(op_keys_of(connection, *seq)?);
                super::upload::confirm_item(connection, *seq, *position, item, [temp.as_str(), row.as_str()])?;
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Begin(record) => {
                if let Some(seq) = record.seq {
                    synced.extend(op_keys_of(connection, seq)?);
                }
                let seq = begin_operation(connection, record)?;
                super::blob::pin(connection, seq, record, models, now_ms)?;
                synced.extend(operation_keys(&record.action_key, &record.row_ids, record.temp_id.as_ref()));
                result.operations.insert(seq);
                wrote = true;
            }
            OperationTransition::Close { seq, status } => {
                let guard = operation_guard(OperationEvent::Close(*status))?;
                let open: Option<i64> = connection.query_row(&format!("SELECT seq FROM op WHERE seq = ?1 AND {guard}"), params![seq], |row| row.get(0)).optional()?;
                if open.is_none() {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                synced.extend(op_keys_of(connection, *seq)?);
                result.operations.extend(super::blob::release(connection, *seq, *status == OperationStatus::Committed)?);
                if *status == OperationStatus::RolledBack {
                    crate::operation::record_transfer_aborts(connection, *seq, None)?;
                }
                delete_operation(connection, *seq)?;
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Fail { seq, status, failure } => {
                synced.extend(op_keys_of(connection, *seq)?);
                if set_open_status(connection, *seq, *status, failure, now_ms)? == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                if *status == OperationStatus::Failed {
                    connection.execute("UPDATE blob_upload SET ready = 0, failed_at = COALESCE(failed_at, ?1) WHERE seq = ?2", params![now_ms, seq])?;
                    if failure.kind == crate::syncstate::FailureKind::Rejected { super::blob::release(connection, *seq, false)?; }
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Start { seq } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let started = connection.execute(
                    &format!("UPDATE op SET transport_started = 1, attempts = attempts + 1, since = ?2 WHERE seq = ?1 AND {}", operation_guard(OperationEvent::Start)?),
                    params![seq, now_ms],
                )?;
                if started == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Postpone { seq, failure } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let [kind, message] = failure_cells(Some(failure));
                let postponed = connection.execute(
                    &format!("UPDATE op SET transport_started = 0, error_kind = ?2, error_message = ?3, since = ?4 WHERE seq = ?1 AND {}", operation_guard(OperationEvent::Postpone)?),
                    params![seq, kind, message, now_ms],
                )?;
                if postponed == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                write_operation_error(connection, *seq, Some(failure))?;
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Progress { seq, item_position, state } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let starts_blob = if matches!(state.stage, crate::upload::UploadStage::Create | crate::upload::UploadStage::Init) && state.put_url.is_none() && state.upload_id.is_none() {
                    let previous_stage: Option<String> = connection.query_row("SELECT stage FROM op_item WHERE seq = ?1 AND position = ?2", params![seq, item_position], |row| row.get(0)).optional()?;
                    previous_stage != crate::operation::enum_text(&state.stage)
                } else { false };
                let saved = crate::operation::write_upload(connection, *seq, *item_position, state)?;
                connection.execute("UPDATE blob_upload SET ready = ?1, failed_at = ?2, signed_id = ?3, total_bytes = ?4, sent_bytes = ?5 WHERE seq = ?6", params![state.transferred, state.failed_at.map(|at| at as i64), state.signed_id, state.total_bytes as i64, state.sent_bytes as i64, seq])?;
                if starts_blob {
                    connection.execute("UPDATE blob_upload SET created_at = ?1 WHERE seq = ?2", params![now_ms, seq])?;
                }
                if saved == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                if matches!(state.phase, crate::upload::UploadStatus::Failed | crate::upload::UploadStatus::Cancelled) {
                    let open: bool = connection.query_row("SELECT EXISTS (SELECT 1 FROM op_item WHERE seq = ?1 AND phase NOT IN ('failed', 'cancelled'))", [seq], |row| row.get(0))?;
                    if !open { result.ended_uploads.push((*seq, state.phase, state.error.clone())); }
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::BackgroundRefused { seq, item_position, detail } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let saved = connection.execute(
                    "UPDATE op_item SET background_refused = ?3 WHERE seq = ?1 AND position = ?2 AND phase NOT IN ('failed', 'cancelled')",
                    params![seq, item_position, detail],
                )?;
                if saved == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                connection.execute("UPDATE blob_upload SET ready = 0, failed_at = COALESCE(failed_at, ?1) WHERE seq = ?2", params![now_ms, seq])?;
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Retry { seq } => {
                super::blob::retry(connection, *seq, models, now_ms)?;
                synced.extend(op_keys_of(connection, *seq)?);
                connection.execute("UPDATE op_item SET background_refused = NULL WHERE seq = ?1", params![seq])?;
                let retried = connection.execute(
                    &format!("UPDATE op SET status = 'pending', transport_started = 0, attempts = 0, error_kind = NULL, error_message = NULL, since = ?2 WHERE seq = ?1 AND {}", operation_guard(OperationEvent::Retry)?),
                    params![seq, now_ms],
                )?;
                if retried == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                connection.execute("DELETE FROM op_item_retry WHERE seq = ?1", [seq])?;
                connection.execute("UPDATE blob_upload SET failed_at = NULL WHERE seq = ?1", [seq])?;
                write_operation_error(connection, *seq, None)?;
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Remove { seq, expected } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let matches = match expected {
                    Some(status) => connection.query_row("SELECT EXISTS (SELECT 1 FROM op WHERE seq = ?1 AND status = ?2)", params![seq, status.as_sql()], |row| row.get(0))?,
                    None => true,
                };
                if matches {
                    super::blob::release(connection, *seq, false)?;
                    crate::operation::record_transfer_aborts(connection, *seq, None)?;
                }
                let removed = if matches { delete_operation(connection, *seq)? } else { 0 };
                if removed == 0 {
                    result.unknown_operation_acks += 1;
                } else {
                    result.operations.insert(*seq);
                    wrote = true;
                }
            }
        }
    }
    Ok(wrote)
}

fn operation_guard(event: OperationEvent) -> Result<String, StoreError> {
    event.guard_sql().ok_or_else(|| StoreError::Constraint(format!("the operation machine accepts {event:?} from no status")))
}

fn set_open_status(connection: &Connection, seq: i64, status: OperationStatus, error: &OperationFailure, now_ms: i64) -> Result<usize, StoreError> {
    let dedupe = if status == OperationStatus::Failed { ", dedupe_key = NULL" } else { "" };
    let [kind, message] = failure_cells(Some(error));
    let changed = connection.execute(
        &format!("UPDATE op SET status = ?1, error_kind = ?2, error_message = ?3, since = ?4{dedupe} WHERE seq = ?5 AND {}", operation_guard(OperationEvent::Fail(status))?),
        params![status.as_sql(), kind, message, now_ms, seq],
    )?;
    if changed > 0 {
        write_operation_error(connection, seq, Some(error))?;
    }
    Ok(changed)
}

pub(super) fn begin_operation(connection: &Connection, record: &OperationRecord) -> Result<i64, StoreError> {
    let dedupe_key = if record.status == OperationStatus::Failed { None } else { record.dedupe_key.clone() };
    connection.execute(
        "INSERT INTO op (seq, action_key, status, temp_id, transport_started, base_revision, created_at, dedupe_key, since) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?7) \
         ON CONFLICT (seq) DO UPDATE SET action_key = excluded.action_key, \
         status = excluded.status, temp_id = excluded.temp_id, \
         transport_started = excluded.transport_started, base_revision = excluded.base_revision, \
         created_at = excluded.created_at, dedupe_key = excluded.dedupe_key",
        params![
            record.seq,
            record.action_key,
            record.status.as_sql(),
            record.temp_id.as_ref().map(|id| id.as_str().to_string()),
            i64::from(record.transport_started),
            record.base_revision,
            record.created_at,
            dedupe_key,
        ],
    )?;
    let seq = record.seq.unwrap_or_else(|| connection.last_insert_rowid());
    write_rows(connection, seq, &record.row_ids)?;
    write_value(connection, seq, "input", Some(&record.input))?;
    let revision = connection.query_row("SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1", [], |row| row.get(0))?;
    crate::store::plan::op_effect::write_prior(connection, seq, revision, record.rollback.as_ref())?;
    if let Some(state) = &record.upload {
        crate::operation::write_upload(connection, seq, 0, state)?;
    }
    Ok(seq)
}

fn write_item_source(connection: &Connection, seq: i64, position: i64, item: &crate::store::UploadAdmission) -> Result<(), StoreError> {
    let (file_id, error) = match &item.source {
        Ok(file) => {
            let mut file = file.clone();
            file.field = format!("op_item.{seq}.{position}");
            crate::files::put(connection, &file)?;
            (Some(file.id), None)
        }
        Err(error) => (None, Some(error)),
    };
    let value = serde_json::to_value(&item.input).map_err(|error| StoreError::from(std::io::Error::other(error)))?;
    crate::operation::write_item_value(connection, seq, position, "input", Some(&value))?;
    connection.execute("UPDATE op_item SET row_id = ?2, file_id = ?3, source_error = ?4, client_key = ?6 WHERE seq = ?1 AND position = ?5", params![seq, item.row.as_str(), file_id, error, position, item.client_key])?;
    Ok(())
}

pub(crate) fn save_pending(connection: &Connection, model: &str, row_id: &str, path: &str, value: &Value) -> Result<(), StoreError> {
    let owner = texts(&[model, row_id, path]);
    delete_trees(connection, "local_pending", "model = ?1 AND row_id = ?2 AND path = ?3", &owner)?;
    Ok(write_tree(connection, "local_pending", &["model", "row_id", "path"], &owner, value)?)
}
