use super::*;

pub(crate) fn apply_envelope(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    envelope: &BatchPlan,
    (now_ms, generation): (i64, Generation),
) -> Result<CommitResult, StoreError> {
    let revision = Reader::new(connection, models).revision_seq()? + 1;
    apply_envelope_at(connection, models, envelope, (now_ms, revision, generation), None, None)
}

pub(in super::super) fn apply_envelope_at(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    planned: &BatchPlan,
    (now_ms, revision, generation): (i64, i64, Generation),
    mut originals: Option<&mut BTreeMap<RowRef, Option<Map<String, Value>>>>,
    mut feed_cache: Option<(&mut Prefetched, &HiddenRows, &mut EventUndo)>,
) -> Result<CommitResult, StoreError> {
    let mut followed = planned.clone();
    super::super::follow_removed::follow_removed_by(connection, models, &mut followed)?;
    super::super::follow_cascade::follow_destroys(connection, models, &mut followed)?;
    followed.reduce_repeats();
    followed.validate(generation)?;
    let envelope = &followed;
    if let Some(originals) = originals.as_deref_mut() {
        let reader = Reader::new(connection, models);
        let mut wanted = BTreeMap::<String, Vec<String>>::new();
        for operation in &envelope.entity_ops {
            let row = operation.row_ref();
            if !originals.contains_key(&row) { wanted.entry(row.model.to_string()).or_default().push(row.id.to_string()); }
        }
        for (model, ids) in wanted {
            let mut rows = reader.stored_rows(&model, &ids)?;
            for id in ids {
                let row_id = crate::identity::RowId::new(&id).map_err(|error| StoreError::Constraint(error.to_string()))?;
                let row = RowRef::new(ModelKey::new(&model), row_id);
                if let Some((_, _, undo)) = feed_cache.as_mut() { undo.originals.push(row.clone()); }
                originals.entry(row).or_insert_with(|| rows.remove(&id).map(|stored| stored.row));
            }
        }
    }
    let result = match feed_cache {
        None => write_followed(connection, models, envelope, (now_ms, revision), originals, None, &mut EventUndo::default())?,
        Some((current, hidden, undo)) => {
            undo.rows.extend(envelope.entity_ops.iter().map(|op| { let row = op.row_ref(); let key = (row.model.to_string(), row.id.to_string()); let before = current.get(&key).cloned(); (key, before) }));
            write_followed(connection, models, envelope, (now_ms, revision), originals, Some((current, hidden)), undo)?
        }
    };
    Ok(result)
}

fn write_followed(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    envelope: &BatchPlan,
    (now_ms, revision): (i64, i64),
    originals: Option<&mut BTreeMap<RowRef, Option<Map<String, Value>>>>,
    mut feed_cache: Option<(&mut Prefetched, &HiddenRows)>,
    undo: &mut EventUndo,
) -> Result<CommitResult, StoreError> {
    let mut wrote = false;
    let mut tombstoned = false;
    let mut files_changed = false;
    let mut changes = RowChanges::default();
    let mut moves: Vec<ScopeOp> = Vec::new();
    let mut heads: Vec<(ScopeRef, crate::identity::RowId)> = Vec::new();
    let mut windows = HashMap::new();
    let mut threshold_windows = HashMap::new();
    let mut destroyed: Vec<Destroyed> = Vec::new();
    let mut replaced: Vec<(RowRef, crate::identity::RowId)> = Vec::new();
    let mut result = CommitResult::noop(envelope.tx_id);
    let lands_rows = envelope.entity_ops.iter().any(|operation| matches!(operation, EntityOp::Upsert { .. })) || !envelope.scope_ops.is_empty();
    let hidden = if let Some((_, hidden)) = &feed_cache { (*hidden).clone() } else if lands_rows { hidden_rows(connection, &envelope.operation_transitions)? } else { HiddenRows::new() };
    let mut local;
    let feed = feed_cache.is_some();
    let prefetched: &mut Prefetched = match feed_cache.as_mut() {
        Some((current, _)) => {
            let missing: Vec<_> = envelope.entity_ops.iter().filter(|op| {
                let row = op.row_ref();
                !current.contains_key(&(row.model.to_string(), row.id.to_string()))
            }).cloned().collect();
            let found = prefetch_stored(connection, models, &missing, &hidden)?;
            current.extend(found);
            &mut **current
        }
        None => {
            local = prefetch_stored(connection, models, &envelope.entity_ops, &hidden)?;
            &mut local
        }
    };
    let mut pending = prefetch_pending(connection, models, &envelope.entity_ops, prefetched)?;
    let mut parents = BTreeSet::new();
    super::super::follow_pointer::affected_parents(models, &envelope.entity_ops, prefetched, &mut parents)?;

    for operation in &envelope.entity_ops {
        match operation {
            EntityOp::Upsert { model, row, .. } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                apply_upsert(connection, info, model, row, envelope, Landing { revision, hidden: &hidden, prefetched: &mut *prefetched, pending: &mut pending, windows: &mut windows, threshold_windows: &mut threshold_windows, result: &mut changes, moves: &mut moves, heads: &mut heads })?;
            }
            EntityOp::Touch { model, id } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                let touched = connection
                    .execute(&format!(include_str!("../sql/apply_envelope/apply_envelope_at_update.sql"), info.table), params![revision, id.as_str()])?;
                if touched > 0 {
                    if let Some(Some(row)) = prefetched.get_mut(&(model.to_string(), id.to_string())) { row.revision = revision; }
                    changes.push(model, id.as_str(), RowChangeKind::Touched, BTreeSet::new());
                }
            }
            EntityOp::Destroy { model, id, reason } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                let removed = remove_row(connection, info, id.as_str())?;
                prefetched.insert((model.to_string(), id.to_string()), None);
                if reason.writes_tombstone() {
                    write_tombstone(connection, model.as_str(), id.as_str(), revision, now_ms)?;
                    tombstoned = true;
                }
                if removed > 0 {
                    changes.push(model, id.as_str(), RowChangeKind::Destroyed, BTreeSet::new());
                    if let DestroyReason::Replaced { by } = reason {
                        replaced.push((RowRef::new(model.clone(), id.clone()), by.clone()));
                    }
                }
                files_changed |= release_files(connection, model, id, reason)?;
                destroyed.push(Destroyed { model: model.clone(), id: id.as_str().to_string(), reason: reason.clone() });
            }
        }
    }

    stamp_confirmed_rows(connection, models, &envelope.operation_transitions, revision)?;
    let mut synced_keys: BTreeSet<SyncKey> = BTreeSet::new();
    wrote |= apply_transitions(connection, &envelope.operation_transitions, now_ms, &mut synced_keys, &mut result)?;
    if let Some(originals) = originals {
        let reader = Reader::new(connection, models);
        let mut wanted = BTreeMap::<String, Vec<String>>::new();
        for (model, id) in &synced_keys {
            if !models.contains_key(model) { continue; }
            let Ok(row_id) = crate::identity::RowId::new(id) else { continue };
            let row = RowRef::new(ModelKey::new(model), row_id);
            if originals.contains_key(&row) { continue; }
            if feed {
                if let Some(stored) = prefetched.get(&(model.clone(), id.clone())) {
                    undo.originals.push(row.clone());
                    originals.insert(row, stored.as_ref().map(|stored| stored.row.clone()));
                    continue;
                }
            }
            wanted.entry(model.clone()).or_default().push(id.clone());
        }
        for (model, ids) in wanted {
            let mut rows = reader.stored_rows_with_revisions(&model, &ids)?;
            for id in ids {
                let row_id = crate::identity::RowId::new(&id).map_err(|error| StoreError::Constraint(error.to_string()))?;
                let stored = rows.remove(&id);
                let row = RowRef::new(ModelKey::new(&model), row_id);
                undo.originals.push(row.clone());
                originals.insert(row, stored.as_ref().map(|row| row.row.clone()));
                if feed {
                    undo.rows.push(((model.clone(), id.clone()), prefetched.get(&(model.clone(), id.clone())).cloned()));
                    prefetched.insert((model.clone(), id), stored);
                }
            }
        }
    }
    super::super::follow_pointer::affected_parents(models, &envelope.entity_ops, prefetched, &mut parents)?;
    result.parents_affected = parents.len();
    let (derived, children_read) = super::super::follow_pointer::derived_parents(&Reader::new(connection, models), models, &parents)?;
    result.pointer_rows_read = children_read;
    for (model, row) in derived {
        let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
        apply_upsert(connection, info, &model, &row, envelope, Landing { revision, hidden: &hidden, prefetched: &mut *prefetched, pending: &mut pending, windows: &mut windows, threshold_windows: &mut threshold_windows, result: &mut changes, moves: &mut moves, heads: &mut heads })?;
    }
    for (model, id) in recompute_sync(connection, &synced_keys)? {
        let touched = changes.changes.iter().any(|change| change.row.model.as_str() == model && change.row.id.as_str() == id);
        if !touched {
            changes.push(&ModelKey::new(&model), &id, RowChangeKind::Synced, BTreeSet::new());
        }
        wrote = true;
    }

    collect_scopes(connection, &changes.changes, &destroyed, &mut result.scopes)?;
    for item in &destroyed {
        let removed = match &item.reason {
            DestroyReason::Replaced { by } => {
                let moved = connection.execute(
                    include_str!("../sql/apply_envelope/apply_envelope_at_update_scope_member.sql"),
                    params![by.as_str(), item.model.as_str(), item.id],
                )?;
                connection.execute(
                    include_str!("../sql/apply_envelope/apply_envelope_at_delete_scope_revision.sql"),
                    params![item.model.as_str(), item.id, by.as_str()],
                )?;
                moved
            }
            _ => connection.execute(
                include_str!("../sql/apply_envelope/apply_envelope_at_delete_scope_member.sql"),
                params![item.model.as_str(), item.id],
            )?,
        };
        if removed > 0 {
            wrote = true;
        }
    }
    wrote |= !changes.changes.is_empty() || tombstoned;

    files_changed |= apply_file_ops(connection, &envelope.file_ops)?;
    wrote |= files_changed;
    for pending in &envelope.pending_locals {
        save_pending(connection, &pending.model, &pending.row_id, &pending.path, &pending.value)?;
    }

    let visible;
    let scope_ops: &[ScopeOp] = if hidden.is_empty() {
        &envelope.scope_ops
    } else {
        visible = visible_scope_ops(&envelope.scope_ops, &hidden);
        &visible
    };
    wrote |= apply_scope_ops(connection, models, &moves, &mut result.scopes, &mut result.members)?;
    wrote |= join_heads(connection, models, &heads, &mut result.scopes, &mut result.members)?;
    wrote |= apply_scope_ops(connection, models, scope_ops, &mut result.scopes, &mut result.members)?;
    let renamed: Vec<RowChange> = replaced
        .iter()
        .map(|(from, by)| RowChange { row: RowRef::new(from.model.clone(), by.clone()), kind: RowChangeKind::Inserted, fields: BTreeSet::new() })
        .collect();
    refresh_places(connection, models, changes.changes.iter().chain(&renamed))?;
    apply_edge_writes(connection, &envelope.edge_writes)?;
    if let Some(window) = &envelope.window {
        save_window(connection, window, envelope.generation, now_ms)?;
    }
    for debt in &envelope.window_debts {
        super::super::windows::mark_families(connection, &[(debt.model.clone(), debt.relation.clone())], debt.since)?;
    }

    if let Some(target) = &envelope.snapshot {
        let reader = Reader::new(connection, models);
        let id = target.id.to_string();
        if let Some(stored) = reader.stored_rows(target.model.as_str(), std::slice::from_ref(&id))?.remove(&id) {
            let sync = crate::store::sync_state::read_sync(connection, target.model.as_str(), std::slice::from_ref(&id))?.remove(&id).unwrap_or_default();
            result.snapshot = Some(std::sync::Arc::new(crate::store::Record { id: stored.id, revision: stored.revision, data: stored.row, sync }));
        }
    }
    for (item, target) in &envelope.item_snapshots {
        let reader = Reader::new(connection, models);
        let id = target.id.to_string();
        let stored = reader.stored_rows(target.model.as_str(), std::slice::from_ref(&id))?.remove(&id);
        let row = if let Some(stored) = stored {
            let sync = crate::store::sync_state::read_sync(connection, target.model.as_str(), std::slice::from_ref(&id))?.remove(&id).unwrap_or_default();
            Some(std::sync::Arc::new(crate::store::Record { id: stored.id, revision: stored.revision, data: stored.row, sync }))
        } else { None };
        result.item_snapshots.push((item.clone(), row));
    }

    result.renewed = changes.renewed;
    if wrote {
        manifest_write(connection, REVISION_SEQ_KEY, &revision.to_string())?;
        result.revision = Some(revision);
        result.rows = changes.changes;
        result.replaced = replaced;
        for removed in destroyed.iter().filter(|removed| removed.reason == DestroyReason::Server) {
            let Ok(id) = crate::identity::RowId::new(&removed.id) else { continue };
            result.server_removed.push(RowRef::new(removed.model.clone(), id));
        }
        result.queue_operations = crate::store::operation::read_operations(connection, &result.operations)?;
    } else {
        result.scopes.clear();
        result.members.clear();
    }
    Ok(result)
}

pub(super) fn save_window(connection: &Connection, window: &WindowSave, generation: Generation, now_ms: i64) -> Result<(), StoreError> {
    let record = WindowRecord { key: window.key.clone(), cursor: window.cursor.clone(), anchor: window.anchor.clone(), has_next: window.has_next, total: window.total, pages: window.pages, issued: window.issued, applied: window.applied, generation: generation.value() as i64 };
    upsert_window(connection, &record, now_ms)?;
    if let Some(since) = window.stale_since {
        connection.execute(include_str!("../sql/apply_envelope/save_window_update_query_window.sql"), params![window.key, since])?;
    }
    if let Some(marker) = &window.marker {
        connection.execute(include_str!("../sql/apply_envelope/save_window_delete_window_marker.sql"), [&window.key])?;
        let mut insert = connection.prepare_cached(include_str!("../sql/apply_envelope/save_window_insert_window_marker.sql"))?;
        for (field, value) in &marker.values {
            insert.execute(params![window.key, generation.value() as i64, marker.sequence, field, value])?;
        }
    }
    if let Some(landed_at) = window.cleared_at {
        connection.execute(include_str!("../sql/apply_envelope/save_window_update_query_window_2.sql"), params![landed_at, window.key, window.applied])?;
    }
    Ok(())
}

pub(super) fn collect_scopes(
    connection: &Connection,
    changes: &[RowChange],
    destroyed: &[Destroyed],
    scopes: &mut BTreeSet<ScopeRef>,
) -> Result<(), StoreError> {
    let mut by_model: BTreeMap<&ModelKey, BTreeSet<&str>> = BTreeMap::new();
    for change in changes {
        if change.kind != RowChangeKind::Inserted {
            by_model.entry(&change.row.model).or_default().insert(change.row.id.as_str());
        }
    }
    for item in destroyed {
        by_model.entry(&item.model).or_default().insert(item.id.as_str());
    }
    for (model, ids) in by_model {
        let ids: Vec<&str> = ids.into_iter().collect();
        for chunk in ids.chunks(CHUNK) {
            let sql = format!(
                include_str!("../sql/apply_envelope/collect_scopes.sql"),
                placeholders(chunk.len())
            );
            let mut statement = connection.prepare_cached(&sql)?;
            let arguments = std::iter::once(model.as_str()).chain(chunk.iter().copied());
            let keys = statement.query_map(params_from_iter(arguments), |row| row.get::<_, String>(0))?;
            for key in keys {
                scopes.insert(ScopeRef::new(model.clone(), crate::identity::ListKey::from_raw(&key?)));
            }
        }
    }
    Ok(())
}

pub(super) fn apply_edge_writes(connection: &Connection, writes: &[EdgeWrite]) -> Result<(), StoreError> {
    for write in writes {
        let key = params![write.scope.model.as_str(), write.scope.key.as_str(), write.row.as_str()];
        connection.execute(include_str!("../sql/apply_envelope/apply_edge_writes_delete_scope_edge.sql"), key)?;
        for (name, value) in &write.values {
            let (whole, fraction, text) = match value {
                EdgeValue::Whole(whole) => (Some(*whole), None, None),
                EdgeValue::Fraction(fraction) => (None, Some(*fraction), None),
                EdgeValue::Text(text) => (None, None, Some(text.as_str())),
            };
            connection.execute(
                include_str!("../sql/apply_envelope/apply_edge_writes_insert_scope_edge.sql"),
                params![write.scope.model.as_str(), write.scope.key.as_str(), write.row.as_str(), name, whole, fraction, text],
            )?;
        }
    }
    Ok(())
}

pub(super) fn refresh_places<'a>(connection: &Connection, models: &BTreeMap<String, ModelInfo>, changes: impl Iterator<Item = &'a RowChange> + Clone) -> Result<(), StoreError> {
    for info in models.values().filter(|info| info.spec.relations.iter().any(|relation| relation.order_stable)) {
        let own: Vec<&RowChange> = changes.clone().filter(|change| change.row.model.as_str() == info.spec.key.as_str() && change.kind != RowChangeKind::Destroyed).collect();
        if !own.is_empty() {
            stable::refresh_places(connection, info, &own)?;
        }
    }
    Ok(())
}

pub(super) fn apply_scope_ops(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    operations: &[ScopeOp],
    scopes: &mut BTreeSet<ScopeRef>,
    members: &mut BTreeSet<RowRef>,
) -> Result<bool, StoreError> {
    let stable_fields = |scope: &ScopeRef| {
        let info = models.get(scope.model.as_str())?;
        info.spec.stable_order(&scope.key).map(|fields| (info, fields))
    };
    let mut wrote = false;
    let reader = Reader::new(connection, models);
    let mut claimed: HashMap<ScopeRef, HashSet<String>> = HashMap::new();
    for operation in operations {
        if let ScopeOp::Move { scope, row, order_key } = operation {
            if stable_fields(scope).is_some() {
                continue;
            }
            claimed.entry(scope.clone()).or_default().insert(order_key.clone());
            connection.execute(
                include_str!("../sql/apply_envelope/apply_scope_ops_update_scope_member.sql"),
                params![format!("~parked~{}", row.as_str()), scope.model.as_str(), scope.key.as_str(), row.as_str()],
            )?;
        }
    }
    let detached = operations.iter().filter(|operation| matches!(operation, ScopeOp::Detach { .. }));
    let retained = operations.iter().filter(|operation| !matches!(operation, ScopeOp::Detach { .. }));
    for operation in detached.chain(retained) {
        let changed = match (operation, stable_fields(operation.scope())) {
            (ScopeOp::Move { .. }, Some(_)) => 0,
            (ScopeOp::Append { scope, row, .. }, Some((info, fields))) => {
                let joined = stable::join(connection, info, fields, scope, row)?;
                for moved in &joined.moved {
                    members.insert(RowRef::new(scope.model.clone(), stored_row_id(moved)?));
                }
                usize::from(joined.joined)
            }
            (ScopeOp::Append { scope, row, order_key }, None) => {
                let none = HashSet::new();
                let wanted = claimed.get(scope).unwrap_or(&none);
                let order_key = reader.free_order_key(scope.model.as_str(), scope.key.as_str(), row.as_str(), order_key, wanted)?;
                connection.execute(
                    include_str!("../sql/apply_envelope/apply_scope_ops_insert_scope_member.sql"),
                    params![scope.model.as_str(), scope.key.as_str(), row.as_str(), order_key],
                )?
            }
            (ScopeOp::Move { scope, row, order_key }, None) => connection.execute(
                include_str!("../sql/apply_envelope/apply_scope_ops_update_scope_member.sql"),
                params![order_key, scope.model.as_str(), scope.key.as_str(), row.as_str()],
            )?,
            (ScopeOp::Detach { scope, row }, _) => connection.execute(
                include_str!("../sql/apply_envelope/apply_scope_ops_delete_scope_member.sql"),
                params![scope.model.as_str(), scope.key.as_str(), row.as_str()],
            )?,
        };
        if changed > 0 {
            wrote = true;
            scopes.insert(operation.scope().clone());
            let (ScopeOp::Append { scope, row, .. } | ScopeOp::Move { scope, row, .. } | ScopeOp::Detach { scope, row }) = operation;
            members.insert(RowRef::new(scope.model.clone(), row.clone()));
        }
    }
    Ok(wrote)
}

pub(super) fn op_keys_of(connection: &Connection, seq: i64) -> Result<Vec<SyncKey>, StoreError> {
    let found: Option<(String, Option<String>)> =
        connection.query_row(include_str!("../sql/apply_envelope/op_keys_of.sql"), params![seq], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
    let Some((action_key, temp_id)) = found else { return Ok(Vec::new()) };
    let row_ids = read_rows(connection, seq)?;
    let temp_id = temp_id.as_deref().map(stored_row_id).transpose()?;
    Ok(operation_keys(&action_key, &row_ids, temp_id.as_ref()))
}

pub(super) fn stamp_confirmed_rows(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    transitions: &[OperationTransition],
    revision: i64,
) -> Result<(), StoreError> {
    for transition in transitions {
        let OperationTransition::Close { seq, status: OperationStatus::Committed, .. } = transition else { continue };
        let Some(snapshot) = crate::store::plan::op_effect::read_prior(connection, *seq)? else { continue };
        for item in snapshot.get("rows").and_then(Value::as_array).into_iter().flatten() {
            let (Some(kind), Some(model), Some(id)) =
                (item.get("kind").and_then(Value::as_str), item.get("model").and_then(Value::as_str), item.get("id").and_then(Value::as_str))
            else {
                continue;
            };
            match (kind, item.get("owned").and_then(Value::as_object), models.get(model)) {
                ("destroyed", _, _) => {
                    connection.execute(include_str!("../sql/apply_envelope/stamp_confirmed_rows.sql"), params![revision, model, id])?;
                }
                ("edited", Some(owned), Some(info)) => stamp_owned_fields(connection, info, id, owned, revision)?,
                _ => {}
            }
        }
    }
    Ok(())
}

pub(super) fn stamp_owned_fields(connection: &Connection, info: &ModelInfo, id: &str, owned: &Map<String, Value>, revision: i64) -> Result<(), StoreError> {
    for (_, column) in info.layout.rev_columns().iter().filter(|(field, _)| owned.contains_key(field)) {
        let column = crate::store::ddl::sql_ident(column);
        connection.prepare_cached(&format!(include_str!("../sql/apply_envelope/stamp_owned_fields.sql"), info.table, column = column))?.execute(params![revision, id])?;
    }
    Ok(())
}

