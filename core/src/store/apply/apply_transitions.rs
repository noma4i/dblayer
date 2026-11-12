use super::*;

pub(super) fn apply_transitions(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    transitions: &[OperationTransition],
    now_ms: i64,
    synced: &mut BTreeSet<SyncKey>,
    result: &mut CommitResult,
    parents: &mut BTreeSet<RowRef>,
) -> Result<bool, StoreError> {
    let mut wrote = false;
    for transition in transitions {
        match transition {
            OperationTransition::Begin(record) => {
                if let Some(seq) = record.seq {
                    synced.extend(op_keys_of(connection, seq)?);
                }
                let seq = begin_operation(connection, record)?;
                synced.extend(operation_keys(&record.action_key, &record.row_ids, record.temp_id.as_ref()));
                result.operations.insert(seq);
                wrote = true;
            }
            OperationTransition::Close { seq, status } => {
                let guard = operation_guard(OperationEvent::Close(*status))?;
                let open: Option<i64> = connection.query_row(&format!(include_str!("../sql/apply_transitions/apply_transitions_select_op_2.sql"), guard = guard), params![seq], |row| row.get(0)).optional()?;
                if open.is_none() {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                synced.extend(op_keys_of(connection, *seq)?);
                destroyed_parents(connection, models, *seq, parents, result)?;
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
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Attempt { seq, attempts, cycle_id, cause } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let written = connection.execute(
                    &format!(include_str!("../sql/apply_transitions/attempt_operation.sql"), operation_guard(OperationEvent::Attempt)?),
                    params![seq, attempts, cycle_id, cause.and_then(|cause| cause.cause_columns().map(|(kind, _)| kind)), cause.and_then(|cause| cause.cause_columns().map(|(_, limit)| limit))],
                )?;
                if written > 0 {
                    result.operations.insert(*seq);
                    wrote = true;
                }
            }
            OperationTransition::Sending { seq } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let written = connection.execute(&format!(include_str!("../sql/apply_transitions/sending_operation.sql"), operation_guard(OperationEvent::Attempt)?), params![seq])?;
                if written > 0 {
                    result.operations.insert(*seq);
                    wrote = true;
                }
            }
            OperationTransition::Requeue { seq } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let written = connection.execute(&format!(include_str!("../sql/apply_transitions/requeue_operation.sql"), operation_guard(OperationEvent::Requeue)?), params![seq])?;
                if written > 0 {
                    result.operations.insert(*seq);
                    wrote = true;
                }
            }
            OperationTransition::Retry { seq } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let retried = connection.execute(
                    &format!(include_str!("../sql/apply_transitions/retry_operation.sql"), operation_guard(OperationEvent::Retry)?),
                    params![seq, now_ms],
                )?;
                if retried == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                write_operation_error(connection, *seq, None)?;
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Remove { seq, expected } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let matches = match expected {
                    Some(status) => connection.query_row(include_str!("../sql/apply_transitions/apply_transitions_select_op.sql"), params![seq, status.as_sql()], |row| row.get(0))?,
                    None => true,
                };
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

fn destroyed_parents(connection: &Connection, models: &BTreeMap<String, ModelInfo>, seq: i64, parents: &mut BTreeSet<RowRef>, result: &mut CommitResult) -> Result<(), StoreError> {
    let Some(prior) = crate::store::plan::op_effect::read_prior(connection, seq)? else { return Ok(()) };
    for row in crate::store::plan::Prior::decode(prior)?.rows {
        if let crate::store::plan::PriorRow::Destroyed { model, before, .. } = row {
            if let Some(info) = models.get(&model) {
                crate::store::follow_pointer::parents_of(&info.spec, &before, parents)?;
                result.stale.extend(crate::store::follow_pointer::named_parents(&Reader::new(connection, models), &info.spec, &before)?);
            }
        }
    }
    Ok(())
}

pub(crate) fn operation_guard(event: OperationEvent) -> Result<String, StoreError> {
    event.guard_sql().ok_or_else(|| StoreError::Constraint(format!("the operation machine accepts {event:?} from no status")))
}

pub(super) fn set_open_status(connection: &Connection, seq: i64, status: OperationStatus, error: &OperationFailure, now_ms: i64) -> Result<usize, StoreError> {
    let dedupe = if status == OperationStatus::Failed { ", dedupe_key = NULL" } else { "" };
    let [kind, message] = failure_cells(Some(error));
    let changed = connection.execute(
        &format!(include_str!("../sql/apply_transitions/set_open_status.sql"), operation_guard(OperationEvent::Fail(status))?, dedupe = dedupe),
        params![status.as_sql(), kind, message, now_ms, seq],
    )?;
    if changed > 0 {
        write_operation_error(connection, seq, Some(error))?;
    }
    Ok(changed)
}

pub(in super::super) fn begin_operation(connection: &Connection, record: &OperationRecord) -> Result<i64, StoreError> {
    let dedupe_key = if record.status == OperationStatus::Failed { None } else { record.dedupe_key.clone() };
    connection.execute(
        include_str!("../sql/apply_transitions/begin_operation_insert_op.sql"),
        params![
            record.seq,
            record.action_key,
            record.status.as_sql(),
            record.temp_id.as_ref().map(|id| id.as_str().to_string()),
            record.base_revision,
            record.created_at,
            dedupe_key,
        ],
    )?;
    let seq = record.seq.unwrap_or_else(|| connection.last_insert_rowid());
    write_rows(connection, seq, &record.row_ids)?;
    write_value(connection, seq, "input", Some(&record.input))?;
    let revision = connection.query_row(include_str!("../sql/apply_transitions/begin_operation_select_manifest.sql"), [], |row| row.get(0))?;
    crate::store::plan::op_effect::write_prior(connection, seq, revision, record.rollback.as_ref())?;
    Ok(seq)
}

pub(crate) fn save_pending(connection: &Connection, model: &str, row_id: &str, path: &str, value: &Value) -> Result<(), StoreError> {
    let owner = texts(&[model, row_id, path]);
    delete_trees(connection, "local_pending", "model = ?1 AND row_id = ?2 AND path = ?3", &owner)?;
    Ok(write_tree(connection, "local_pending", &["model", "row_id", "path"], &owner, value)?)
}
