pub(crate) fn apply_envelope(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    envelope: &BatchPlan,
    now_ms: i64,
) -> Result<CommitResult, StoreError> {
    let revision = Reader::new(connection, models).revision_seq()? + 1;
    apply_envelope_at(connection, models, envelope, now_ms, revision, None)
}

pub(super) fn apply_envelope_at(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    planned: &BatchPlan,
    now_ms: i64,
    revision: i64,
    mut originals: Option<&mut BTreeMap<RowRef, Option<Map<String, Value>>>>,
) -> Result<CommitResult, StoreError> {
    let mut followed = planned.clone();
    super::follow_removed::follow_removed_by(connection, models, &mut followed)?;
    super::follow_cascade::follow_destroys(connection, models, &mut followed)?;
    super::follow_pointer::follow_parent_pointers(connection, models, &mut followed)?;
    let envelope = &followed;
    if let Some(originals) = originals.as_deref_mut() {
        let reader = Reader::new(connection, models);
        for operation in &envelope.entity_ops {
            let row = operation.row_ref();
            if !originals.contains_key(&row) {
                originals.insert(row.clone(), reader.point(row.model.as_str(), row.id.as_str())?);
            }
        }
    }
    let mut wrote = false;
    let mut tombstoned = false;
    let mut files_changed = false;
    let mut changes = RowChanges::default();
    let mut moves: Vec<ScopeOp> = Vec::new();
    let mut heads: Vec<(ScopeRef, crate::identity::RowId)> = Vec::new();
    let mut destroyed: Vec<Destroyed> = Vec::new();
    let mut replaced: Vec<(RowRef, crate::identity::RowId)> = Vec::new();
    let mut result = CommitResult::noop(envelope.tx_id);
    let lands_rows = envelope.entity_ops.iter().any(|operation| matches!(operation, EntityOp::Upsert { .. })) || !envelope.scope_ops.is_empty();
    let hidden = if lands_rows { hidden_rows(connection, &envelope.operation_transitions)? } else { HiddenRows::new() };
    let mut prefetched = prefetch_stored(connection, models, &envelope.entity_ops, &hidden)?;

    for operation in &envelope.entity_ops {
        match operation {
            EntityOp::Upsert { model, row, .. } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                apply_upsert(connection, info, model, row, envelope, Landing { revision, hidden: &hidden, prefetched: &mut prefetched, result: &mut changes, moves: &mut moves, heads: &mut heads })?;
            }
            EntityOp::Touch { model, id } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                let touched = connection
                    .execute(&format!("UPDATE {} SET revision = ?1 WHERE id = ?2", info.table), params![revision, id.as_str()])?;
                if touched > 0 {
                    changes.push(model, id.as_str(), RowChangeKind::Touched, BTreeSet::new());
                }
            }
            EntityOp::Destroy { model, id, reason } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                let removed = remove_row(connection, info, id.as_str())?;
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
    wrote |= apply_transitions(connection, models, envelope.generation, &envelope.operation_transitions, now_ms, &mut synced_keys, &mut result)?;
    if let Some(originals) = originals {
        let reader = Reader::new(connection, models);
        for (model, id) in &synced_keys {
            if !models.contains_key(model) { continue; }
            let Ok(id) = crate::identity::RowId::new(id) else { continue };
            let row = RowRef::new(ModelKey::new(model), id);
            if !originals.contains_key(&row) {
                originals.insert(row.clone(), reader.point(model, row.id.as_str())?);
            }
        }
    }
    let is_batch = |key: &str| crate::operation::Operation::upload_action(key).is_some_and(|(model, action)| models.get(model.as_str()).is_some_and(|info| info.spec.upload_action(&action).is_some_and(|declared| declared.batch.is_some())));
    for (model, id) in recompute_sync_with(connection, &synced_keys, &is_batch)? {
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
                    "UPDATE OR REPLACE scope_member SET row_id = ?1 WHERE model = ?2 AND row_id = ?3",
                    params![by.as_str(), item.model.as_str(), item.id],
                )?;
                connection.execute(
                    "DELETE FROM scope_revision WHERE model = ?1 AND row_id = ?2 AND scope_key IN (SELECT scope_key FROM scope_member WHERE model = ?1 AND row_id = ?3)",
                    params![item.model.as_str(), item.id, by.as_str()],
                )?;
                moved
            }
            _ => connection.execute(
                "DELETE FROM scope_member WHERE model = ?1 AND row_id = ?2",
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
        super::windows::mark_families(connection, &[(debt.model.clone(), debt.relation.clone())], debt.since)?;
    }

    if let Some(target) = &envelope.snapshot {
        let reader = Reader::new(connection, models);
        let id = target.id.to_string();
        if let Some(stored) = reader.stored_rows(target.model.as_str(), std::slice::from_ref(&id))?.remove(&id) {
            let sync = crate::syncstate::read_sync(connection, target.model.as_str(), std::slice::from_ref(&id))?.remove(&id).unwrap_or_default();
            result.snapshot = Some(std::sync::Arc::new(crate::store::Record { id: stored.id, revision: stored.revision, data: stored.row, sync }));
        }
    }
    for (item, target) in &envelope.item_snapshots {
        let reader = Reader::new(connection, models);
        let id = target.id.to_string();
        let stored = reader.stored_rows(target.model.as_str(), std::slice::from_ref(&id))?.remove(&id);
        let row = if let Some(stored) = stored {
            let sync = crate::syncstate::read_sync(connection, target.model.as_str(), std::slice::from_ref(&id))?.remove(&id).unwrap_or_default();
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
        let mut progress_only: BTreeSet<_> = envelope.operation_transitions.iter().filter_map(|transition| match transition {
            OperationTransition::Progress { seq, state, .. } if state.phase != crate::upload::UploadStatus::Queued => Some(*seq),
            _ => None,
        }).collect();
        for transition in &envelope.operation_transitions {
            let seq = match transition {
                OperationTransition::Begin(record) | OperationTransition::BeginUpload { record, .. } | OperationTransition::BeginBlob { record, .. } => record.seq,
                OperationTransition::RetireBlob { .. } => None,
                OperationTransition::Progress { seq, state, .. } if state.phase == crate::upload::UploadStatus::Queued => Some(*seq),
                OperationTransition::Progress { .. } | OperationTransition::BackgroundRefused { .. } => None,
                OperationTransition::RemoveUploadItem { seq, .. } | OperationTransition::ReplaceUploadSource { seq, .. } | OperationTransition::ConfirmUploadItem { seq, .. }
                | OperationTransition::Close { seq, .. } | OperationTransition::Fail { seq, .. }
                | OperationTransition::Start { seq } | OperationTransition::Postpone { seq, .. }
                | OperationTransition::Retry { seq } | OperationTransition::Remove { seq, .. } => Some(*seq),
            };
            if let Some(seq) = seq { progress_only.remove(&seq); }
        }
        for seq in result.operations.iter().filter(|seq| !progress_only.contains(seq)) {
            result.queue_operations.insert(*seq, crate::operation::read_queue_operation(connection, *seq)?);
        }
    } else {
        result.scopes.clear();
        result.members.clear();
    }
    Ok(result)
}

fn save_window(connection: &Connection, window: &WindowSave, generation: Generation, now_ms: i64) -> Result<(), StoreError> {
    let kept = match &window.tail {
        Some(tail) => connection.query_row(
            "SELECT 1 FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND row_id = ?3",
            params![window.scope.model.as_str(), window.scope.key.as_str(), tail.anchor], |_| Ok(()),
        ).optional()?.map(|_| tail),
        None => None,
    };
    let (cursor, has_next) = kept.map_or((window.cursor.clone(), window.has_next), |tail| (tail.cursor.clone(), tail.has_next));
    let record = WindowRecord { key: window.key.clone(), cursor, has_next, total: window.total, pages: window.pages, issued: window.issued, applied: window.applied, generation: generation.value() as i64 };
    upsert_window(connection, &record, now_ms)?;
    if let Some(since) = window.stale_since {
        connection.execute("UPDATE query_window SET stale_since = MAX(COALESCE(stale_since, 0), ?2) WHERE key = ?1", params![window.key, since])?;
    }
    if let Some(marker) = &window.marker {
        connection.execute("DELETE FROM window_marker WHERE window_key = ?1", [&window.key])?;
        let mut insert = connection.prepare_cached("INSERT INTO window_marker (window_key, generation, sequence, field, value) VALUES (?1, ?2, ?3, ?4, ?5)")?;
        for (field, value) in &marker.values {
            insert.execute(params![window.key, generation.value() as i64, marker.sequence, field, value])?;
        }
    }
    if let Some(landed_at) = window.cleared_at {
        connection.execute("UPDATE query_window SET stale_since = CASE WHEN stale_since <= ?3 THEN NULL ELSE stale_since END, landed_at = ?1 WHERE key = ?2", params![landed_at, window.key, window.applied])?;
    }
    Ok(())
}

fn collect_scopes(
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
                "SELECT DISTINCT scope_key FROM scope_member WHERE model = ? AND row_id IN ({})",
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

fn apply_edge_writes(connection: &Connection, writes: &[EdgeWrite]) -> Result<(), StoreError> {
    for write in writes {
        let key = params![write.scope.model.as_str(), write.scope.key.as_str(), write.row.as_str()];
        connection.execute("DELETE FROM scope_edge WHERE model = ?1 AND scope_key = ?2 AND row_id = ?3", key)?;
        for (name, value) in &write.values {
            let (whole, fraction, text) = match value {
                EdgeValue::Whole(whole) => (Some(*whole), None, None),
                EdgeValue::Fraction(fraction) => (None, Some(*fraction), None),
                EdgeValue::Text(text) => (None, None, Some(text.as_str())),
            };
            connection.execute(
                "INSERT INTO scope_edge (model, scope_key, row_id, name, whole, fraction, text) \
                 SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7 WHERE EXISTS (SELECT 1 FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND row_id = ?3)",
                params![write.scope.model.as_str(), write.scope.key.as_str(), write.row.as_str(), name, whole, fraction, text],
            )?;
        }
    }
    Ok(())
}

fn refresh_places<'a>(connection: &Connection, models: &BTreeMap<String, ModelInfo>, changes: impl Iterator<Item = &'a RowChange> + Clone) -> Result<(), StoreError> {
    for info in models.values().filter(|info| info.spec.relations.iter().any(|relation| relation.order_stable)) {
        let own: Vec<&RowChange> = changes.clone().filter(|change| change.row.model.as_str() == info.spec.key.as_str() && change.kind != RowChangeKind::Destroyed).collect();
        if !own.is_empty() {
            stable::refresh_places(connection, info, &own)?;
        }
    }
    Ok(())
}

fn apply_scope_ops(
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
                "UPDATE scope_member SET order_key = ?1 WHERE model = ?2 AND scope_key = ?3 AND row_id = ?4",
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
                    "INSERT INTO scope_member (model, scope_key, row_id, order_key) VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT (model, scope_key, row_id) DO UPDATE SET order_key = excluded.order_key \
                     WHERE order_key IS NOT excluded.order_key",
                    params![scope.model.as_str(), scope.key.as_str(), row.as_str(), order_key],
                )?
            }
            (ScopeOp::Move { scope, row, order_key }, None) => connection.execute(
                "UPDATE scope_member SET order_key = ?1 WHERE model = ?2 AND scope_key = ?3 AND row_id = ?4",
                params![order_key, scope.model.as_str(), scope.key.as_str(), row.as_str()],
            )?,
            (ScopeOp::Detach { scope, row }, _) => connection.execute(
                "DELETE FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND row_id = ?3",
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

fn op_keys_of(connection: &Connection, seq: i64) -> Result<Vec<SyncKey>, StoreError> {
    let found: Option<(String, Option<String>)> =
        connection.query_row("SELECT action_key, temp_id FROM op WHERE seq = ?1", params![seq], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
    let Some((action_key, temp_id)) = found else { return Ok(Vec::new()) };
    let row_ids = read_rows(connection, seq)?;
    let temp_id = temp_id.as_deref().map(stored_row_id).transpose()?;
    Ok(operation_keys(&action_key, &row_ids, temp_id.as_ref()))
}

fn stamp_confirmed_rows(
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
                    connection.execute("UPDATE tombstone SET seq = ?1 WHERE model = ?2 AND id = ?3", params![revision, model, id])?;
                }
                ("edited", Some(owned), Some(info)) => stamp_owned_fields(connection, info, id, owned, revision)?,
                _ => {}
            }
        }
    }
    Ok(())
}

fn stamp_owned_fields(connection: &Connection, info: &ModelInfo, id: &str, owned: &Map<String, Value>, revision: i64) -> Result<(), StoreError> {
    for (_, column) in info.layout.rev_columns().iter().filter(|(field, _)| owned.contains_key(field)) {
        let column = crate::ddl::sql_ident(column);
        connection.prepare_cached(&format!("UPDATE {} SET {column} = ?1 WHERE id = ?2 AND {column} IS NOT NULL", info.table))?.execute(params![revision, id])?;
    }
    Ok(())
}

