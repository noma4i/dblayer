use super::*;

pub(super) fn diff_fields(old: &Map<String, Value>, new: &Map<String, Value>) -> BTreeSet<String> {
    let mut changed = BTreeSet::new();
    for (key, value) in new {
        if old.get(key) != Some(value) {
            changed.insert(key.clone());
        }
    }
    for key in old.keys() {
        if !new.contains_key(key) {
            changed.insert(key.clone());
        }
    }
    changed
}

pub(crate) type HiddenRows = HashSet<(String, String)>;
pub(super) type OpenDestroy = (i64, String);

pub(crate) fn hidden_rows(connection: &Connection, transitions: &[OperationTransition]) -> Result<HiddenRows, StoreError> {
    let mut released: HashSet<i64> = HashSet::new();
    let mut rewritten: HashMap<i64, &OperationRecord> = HashMap::new();
    for transition in transitions {
        match transition {
            OperationTransition::Close { seq, status, .. } if status.is_terminal() => {
                released.insert(*seq);
            }
            OperationTransition::Fail { seq, status: OperationStatus::Failed, .. } | OperationTransition::Remove { seq, .. } => {
                released.insert(*seq);
            }
            OperationTransition::Begin(record) => {
                if let Some(seq) = record.seq {
                    rewritten.insert(seq, record);
                }
            }
            _ => {}
        }
    }
    let mut statement = connection.prepare_cached(include_str!("../sql/apply_rows/hidden_rows.sql"))?;
    let open: Vec<OpenDestroy> = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
    let mut hidden = HiddenRows::new();
    for (seq, action_key) in open {
        if released.contains(&seq) {
            continue;
        }
        let model = action_key.split(':').next().unwrap_or_default().to_string();
        let ids: Vec<String> = match rewritten.get(&seq) {
            Some(record) => record.row_ids.iter().map(|id| id.to_string()).collect(),
            None => read_rows(connection, seq)?.into_iter().map(|id| id.to_string()).collect(),
        };
        hidden.extend(ids.into_iter().map(|id| (model.clone(), id)));
        let snapshot = crate::store::plan::op_effect::read_prior(connection, seq)?;
        let rows = snapshot.as_ref().and_then(|snapshot| snapshot.get("rows")).and_then(Value::as_array);
        for item in rows.into_iter().flatten() {
            let owner = item.get("model").and_then(Value::as_str);
            let id = item.get("id").and_then(Value::as_str);
            if let (Some(owner), Some(id)) = (owner, id) {
                hidden.insert((owner.to_string(), id.to_string()));
            }
        }
    }
    Ok(hidden)
}

pub(super) fn visible_scope_ops(operations: &[ScopeOp], hidden: &HiddenRows) -> Vec<ScopeOp> {
    let is_hidden = |scope: &ScopeRef, row: &crate::identity::RowId| hidden.contains(&(scope.model.to_string(), row.to_string()));
    operations
        .iter()
        .filter_map(|operation| match operation {
            ScopeOp::Append { scope, row, .. } | ScopeOp::Move { scope, row, .. } if is_hidden(scope, row) => None,
            other => Some(other.clone()),
        })
        .collect()
}

pub(super) type PendingRows = HashMap<(String, String), Vec<(String, Value)>>;

pub(super) fn prefetch_pending(connection: &Connection, models: &BTreeMap<String, ModelInfo>, operations: &[EntityOp], prefetched: &Prefetched) -> Result<PendingRows, StoreError> {
    let mut wanted = BTreeMap::<String, Vec<String>>::new();
    for operation in operations {
        let EntityOp::Upsert { model, id, .. } = operation else { continue };
        if models.get(model.as_str()).is_some_and(|info| info.spec.has_local()) && prefetched.get(&(model.to_string(), id.to_string())).is_some_and(Option::is_none) {
            wanted.entry(model.to_string()).or_default().push(id.to_string());
        }
    }
    let mut found = PendingRows::new();
    for (model, ids) in wanted {
        let args = [rusqlite::types::Value::Text(model), rusqlite::types::Value::Text(serde_json::to_string(&ids)?)];
        for (owner, value) in read_trees(connection, "local_pending", &["model", "row_id", "path"], "model = ?1 AND row_id IN (SELECT value FROM json_each(?2))", &args)? {
            if let [rusqlite::types::Value::Text(model), rusqlite::types::Value::Text(id), rusqlite::types::Value::Text(path)] = owner.as_slice() {
                found.entry((model.clone(), id.clone())).or_default().push((path.clone(), value));
            }
        }
    }
    Ok(found)
}

fn take_pending(connection: &Connection, info: &ModelInfo, model: &ModelKey, id: &str, row: &mut Map<String, Value>, pending: &mut PendingRows) -> Result<(), StoreError> {
    let Some(values) = pending.remove(&(model.to_string(), id.to_string())) else { return Ok(()) };
    for (path, value) in values {
        if let Some(slot) = info.spec.local_slot(&path) { slot.write(row, value); }
    }
    delete_trees(connection, "local_pending", "model = ?1 AND row_id = ?2", &texts(&[model.as_str(), id]))?;
    Ok(())
}

pub(super) fn without_nulls(info: &ModelInfo, row: &Map<String, Value>) -> Map<String, Value> {
    row.iter().filter(|(key, value)| !value.is_null() || info.spec.field(key).is_some_and(|field| field.nullable && field.default.is_some())).map(|(key, value)| (key.clone(), value.clone())).collect()
}

pub(crate) type Prefetched = HashMap<(String, String), Option<StoredRow>>;

pub(super) fn prefetch_stored(connection: &Connection, models: &BTreeMap<String, ModelInfo>, operations: &[EntityOp], hidden: &HiddenRows) -> Result<Prefetched, StoreError> {
    let mut wanted: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for operation in operations {
        let EntityOp::Upsert { model, id, .. } = operation else { continue };
        if !id.as_str().is_empty() && !hidden.contains(&(model.to_string(), id.as_str().to_string())) {
            wanted.entry(model.to_string()).or_default().push(id.as_str().to_string());
        }
    }
    let mut found = Prefetched::new();
    for (model, ids) in wanted {
        let Some(info) = models.get(&model) else { continue };
        let mut stored = fetch_stored(connection, info, &ids, true)?;
        for id in ids {
            let row = stored.remove(&id);
            found.insert((model.clone(), id), row);
        }
    }
    Ok(found)
}

pub(super) struct Landing<'a> {
    pub(super) revision: i64,
    pub(super) hidden: &'a HiddenRows,
    pub(super) prefetched: &'a mut Prefetched,
    pub(super) pending: &'a mut PendingRows,
    pub(super) windows: &'a mut HashMap<String, bool>,
    pub(super) threshold_windows: &'a mut HashMap<String, Vec<String>>,
    pub(super) result: &'a mut RowChanges,
    pub(super) moves: &'a mut Vec<ScopeOp>,
    pub(super) heads: &'a mut Vec<(ScopeRef, crate::identity::RowId)>,
}

pub(super) fn apply_upsert(
    connection: &Connection,
    info: &ModelInfo,
    model: &ModelKey,
    row: &Map<String, Value>,
    envelope: &BatchPlan,
    landing: Landing<'_>,
) -> Result<(), StoreError> {
    let Landing { revision, hidden, prefetched, pending, windows, threshold_windows, result, moves, heads } = landing;
    let Some(id) = row.get("id").and_then(Value::as_str).filter(|id| !id.is_empty()).map(str::to_string) else { return Ok(()) };
    if hidden.contains(&(model.to_string(), id.clone())) {
        return Ok(());
    }
    let existing = match prefetched.remove(&(model.to_string(), id.clone())) {
        Some(found) => found,
        None => fetch_stored(connection, info, std::slice::from_ref(&id), true)?.remove(&id),
    };
    let old_row = existing.as_ref().map(|stored| &stored.row);
    let mut next = without_nulls(info, row);
    canonicalize(info, &mut next);
    if !envelope.writes_local && info.spec.has_local() {
        match old_row {
            Some(old) => info.spec.keep_local(old, &mut next),
            None => {
                take_pending(connection, info, model, &id, &mut next, pending)?;
            }
        }
    }
    let row = &next;
    declared_membership(connection, info, model, &id, RowVersions { old: old_row, new: row }, envelope, MembershipLog { moves: &mut *moves, heads: &mut *heads, windows, threshold_windows })?;
    let (born_rev, mut field_rev, changed, kind) = match &existing {
        Some(stored) => {
            let changed = diff_fields(&stored.row, row);
            if changed.is_empty() {
                if info.spec.is_memory() && info.spec.expire_window().is_some() {
                    connection.execute(&format!(include_str!("../sql/apply_rows/apply_upsert.sql"), info.table), params![revision, id])?;
                }
                if let Ok(row_id) = crate::identity::RowId::new(&id) {
                    result.renewed.push(RowRef::new(model.clone(), row_id));
                }
                prefetched.insert((model.to_string(), id.clone()), existing.clone());
                return Ok(());
            }
            (stored.born_rev, stored.field_rev.clone(), changed, RowChangeKind::Updated)
        }
        None => (revision, BTreeMap::new(), row.keys().cloned().collect(), RowChangeKind::Inserted),
    };
    for field in &changed {
        field_rev.insert(field.clone(), revision);
    }
    field_rev.retain(|field, _| row.contains_key(field));
    write_row(connection, info, &id, row, RowRevisions { exists: existing.is_some(), changed: &changed, field_rev: &field_rev, revision, born_rev })?;
    if !info.search.is_empty() && (existing.is_none() || touches_search(info, &changed)) {
        reindex(connection, info, &id, row)?;
    }
    if let Some(old) = old_row {
        membership_moves(info, model, &id, old, row, moves, heads);
    }
    let row_id = crate::identity::RowId::new(&id).map_err(|error| StoreError::Constraint(error.to_string()))?;
    prefetched.insert((model.to_string(), id.clone()), Some(StoredRow { id: row_id, revision, born_rev, field_rev, row: row.clone() }));
    result.push(model, &id, kind, changed);
    Ok(())
}

pub(super) fn canonicalize(info: &ModelInfo, row: &mut Map<String, Value>) {
    for plan in &info.layout.fields {
        let Some(value) = row.get(plan.field()) else { continue };
        let canonical = match plan {
            FieldPlan::Leaf { leaf, .. } => normalize_leaf(leaf, value),
            FieldPlan::Typed { member, .. } => normalize(member, &info.layout, value),
            FieldPlan::Rest { .. } => continue,
        };
        match canonical {
            Some(canonical) => row.insert(plan.field().to_string(), canonical),
            None => row.remove(plan.field()),
        };
    }
}

pub(super) struct RowRevisions<'a> {
    pub(super) exists: bool,
    pub(super) changed: &'a BTreeSet<String>,
    pub(super) field_rev: &'a BTreeMap<String, i64>,
    pub(super) revision: i64,
    pub(super) born_rev: i64,
}

pub(super) fn write_row(connection: &Connection, info: &ModelInfo, id: &str, row: &Map<String, Value>, revisions: RowRevisions<'_>) -> Result<(), StoreError> {
    let RowRevisions { exists, changed, field_rev, revision, born_rev } = revisions;
    let layout = &info.layout;
    let mut names: Vec<String> = vec!["revision".into()];
    let mut values: Vec<rusqlite::types::Value> = vec![revision.into()];
    let writes = |field: &str| !exists || changed.contains(field);
    let mut rewritten: Vec<&FieldPlan> = Vec::new();
    let mut free: Vec<&String> = Vec::new();
    for plan in layout.fields.iter().filter(|plan| writes(plan.field())) {
        match plan {
            FieldPlan::Leaf { field, leaf } => {
                names.push(leaf.column.clone());
                values.push(column_value(leaf.ty, row.get(field)));
                if let Some(column) = &leaf.null {
                    names.push(column.name.clone());
                    values.push(null_marker(row.get(field)));
                }
            }
            FieldPlan::Typed { field, member, .. } => {
                let mut columns = Vec::new();
                member_values(member, row.get(field), &mut columns);
                for (name, value) in columns {
                    names.push(name);
                    values.push(value);
                }
                rewritten.push(plan);
            }
            FieldPlan::Rest { field } => free.push(field),
        }
    }
    for (field, column) in layout.rev_columns().iter().filter(|(field, _)| writes(field)) {
        names.push(column.clone());
        values.push(field_rev.get(field).map_or(rusqlite::types::Value::Null, |stamp| (*stamp).into()));
    }
    if exists {
        let assignments: Vec<String> = names.iter().map(|name| format!("{} = ?", crate::store::ddl::sql_ident(name))).collect();
        values.push(id.to_string().into());
        connection.prepare_cached(&format!(include_str!("../sql/apply_rows/write_row_update.sql"), info.table, assignments.join(", ")))?.execute(params_from_iter(values))?;
    } else {
        names.splice(0..0, ["id".to_string(), "born_rev".to_string()]);
        values.splice(0..0, [id.to_string().into(), born_rev.into()]);
        connection.prepare_cached(&format!(include_str!("../sql/apply_rows/write_row_insert.sql"), info.table, names.iter().map(|name| crate::store::ddl::sql_ident(name)).collect::<Vec<_>>().join(", "), placeholders(names.len())))?.execute(params_from_iter(values))?;
    }
    for plan in rewritten {
        let FieldPlan::Typed { field, member, tables } = plan else { continue };
        if exists {
            delete_tables(connection, layout, tables, id)?;
        }
        insert_children(connection, layout, member, row.get(field), id, 0, &mut vec![0; layout.tables.len()])?;
    }
    if let Some(table) = &layout.value_table {
        for field in free {
            let owner = texts(&[id, field]);
            if exists {
                delete_trees(connection, table, "row_id = ?1 AND field = ?2", &owner)?;
            }
            if let Some(value) = row.get(field) {
                write_tree(connection, table, &["row_id", "field"], &owner, value)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn remove_row(connection: &Connection, info: &ModelInfo, id: &str) -> Result<usize, StoreError> {
    unindex(connection, info, id)?;
    let all: Vec<usize> = (0..info.layout.tables.len()).collect();
    delete_tables(connection, &info.layout, &all, id)?;
    if let Some(table) = &info.layout.value_table {
        delete_trees(connection, table, "row_id = ?1", &texts(&[id]))?;
    }
    Ok(connection.execute(&format!(include_str!("../sql/apply_rows/remove_row.sql"), info.table), params![id])?)
}

pub(super) fn membership_moves(info: &ModelInfo, model: &ModelKey, id: &str, old: &Map<String, Value>, new: &Map<String, Value>, moves: &mut Vec<ScopeOp>, heads: &mut Vec<(ScopeRef, crate::identity::RowId)>) {
    let selections = |row: &Map<String, Value>| info.spec.relations.iter().filter(|relation| relation.membership.is_empty() && !relation.joins_node && relation.lists_every_row_of_its_params()).flat_map(|relation| relation.row_params(row).into_iter().filter_map(|params| info.spec.selection_key(&relation.name, &params).ok())).collect::<Vec<_>>();
    let before = selections(old);
    let after = selections(new);
    let Ok(row) = crate::identity::RowId::new(id) else { return };
    moves.extend(
        before.iter().filter(|key| !after.contains(key)).map(|key| ScopeOp::Detach { scope: ScopeRef::new(model.clone(), key.clone()), row: row.clone() }),
    );
    heads.extend(after.iter().filter(|key| !before.contains(key)).map(|key| (ScopeRef::new(model.clone(), key.clone()), row.clone())));
}

pub(super) struct RowVersions<'a> {
    pub(super) old: Option<&'a Map<String, Value>>,
    pub(super) new: &'a Map<String, Value>,
}

pub(super) struct MembershipLog<'a> {
    pub(super) moves: &'a mut Vec<ScopeOp>,
    pub(super) heads: &'a mut Vec<(ScopeRef, crate::identity::RowId)>,
    pub(super) windows: &'a mut HashMap<String, bool>,
    pub(super) threshold_windows: &'a mut HashMap<String, Vec<String>>,
}

pub(super) fn declared_membership(connection: &Connection, info: &ModelInfo, model: &ModelKey, id: &str, versions: RowVersions<'_>, envelope: &BatchPlan, log: MembershipLog<'_>) -> Result<(), StoreError> {
    let RowVersions { old, new } = versions;
    let MembershipLog { moves, heads, windows, threshold_windows } = log;
    use crate::schema::{RemoteShape, PageDirection};
    let row = crate::identity::RowId::new(id).map_err(|error| StoreError::Constraint(error.to_string()))?;
    for relation in info.spec.relations.iter().filter(|relation| relation.constants_described()) {
        let event = relation.joins_events && envelope.server_rows && (!relation.membership.is_empty() || relation.has_threshold());
        let bound = relation.text_params.iter().all(|parameter| {
            relation.membership.contains_key(parameter) || relation.row_rules.iter().any(|rule| matches!(&rule.test, crate::schema::RowTest::GreaterThan { parameter: bound } if bound == parameter))
        });
        let response = relation.joins_events && envelope.response_rows && bound;
        let joins = event || response || envelope.node_reread && relation.joins_node;
        if !joins && !(old.is_some() && bound && (!relation.membership.is_empty() || !relation.row_rules.is_empty())) { continue; }
        if event || envelope.node_reread && relation.joins_node {
            for rule in relation.membership.values() {
                let field = match rule { crate::schema::Membership::Contains(field) | crate::schema::Membership::Equals(field) => field };
                if !new.contains_key(field) { return Err(StoreError::Constraint(format!("missing membership field {field}"))); }
            }
        }
        let mut keys = |values: &Map<String, Value>| if relation.has_threshold() {
            loaded_threshold_scopes(connection, info, relation, values, threshold_windows)
        } else {
            relation.row_params(values).into_iter().map(|params| info.spec.selection_key(&relation.name, &params).map_err(|error| StoreError::Constraint(error.to_string()))).collect::<Result<BTreeSet<_>, _>>()
        };
        let before = old.map(&mut keys).transpose()?.unwrap_or_default();
        let after = keys(new)?;
        moves.extend(before.difference(&after).map(|key| ScopeOp::Detach { scope: ScopeRef::new(model.clone(), key.clone()), row: row.clone() }));
        if !joins { continue; }
        let direction = match relation.remote.as_ref().map(|remote| &remote.shape) { Some(RemoteShape::Connection { direction, .. }) => *direction, _ => PageDirection::Forward };
        for key in after {
            let window = super::super::windows::window_key(key.as_str(), direction);
            let loaded = match windows.get(&window) { Some(loaded) => *loaded, None => { let loaded = connection.query_row(include_str!("../sql/apply_rows/declared_membership.sql"), [&window], |_| Ok(())).optional()?.is_some(); windows.insert(window, loaded); loaded } };
            if loaded { heads.push((ScopeRef::new(model.clone(), key), row.clone())); }
        }
    }
    Ok(())
}

pub(super) fn loaded_threshold_scopes(connection: &Connection, info: &ModelInfo, relation: &crate::schema::RelationSpec, values: &Map<String, Value>, cache: &mut HashMap<String, Vec<String>>) -> Result<BTreeSet<crate::identity::ListKey>, StoreError> {
    let constraint = |message: String| StoreError::Constraint(message);
    let wanted = relation.row_params(values).iter().map(|params| info.spec.partial_selection_params(&relation.name, params).map_err(|error| constraint(error.to_string()))).collect::<Result<Vec<_>, _>>()?;
    let mut keys = BTreeSet::new();
    if wanted.is_empty() { return Ok(keys); }
    let pattern = super::super::windows::like_prefix(&super::super::windows::family_prefix(info.spec.key.as_str(), &relation.name));
    if !cache.contains_key(&pattern) {
        let mut statement = connection.prepare_cached(include_str!("../sql/apply_rows/loaded_threshold_scopes.sql"))?;
        let windows = statement.query_map([&pattern], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
        cache.insert(pattern.clone(), windows);
    }
    for window in &cache[&pattern] {
        let (scope, _) = window.rsplit_once(':').ok_or_else(|| constraint(format!("malformed window key {window}")))?;
        let key = crate::identity::ListKey::from_raw(scope);
        let segments = key.decompose().ok_or_else(|| constraint(format!("malformed window key {window}")))?;
        let params: BTreeMap<String, String> = segments.get(2).and_then(|text| serde_json::from_str(text).ok()).ok_or_else(|| constraint(format!("malformed window params {window}")))?;
        if wanted.iter().any(|partial| partial.iter().all(|(name, value)| params.get(name) == Some(value))) && relation.row_passes_thresholds(values, &params) {
            keys.insert(key);
        }
    }
    Ok(keys)
}

pub(super) fn join_heads(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    heads: &[(ScopeRef, crate::identity::RowId)],
    scopes: &mut BTreeSet<ScopeRef>,
    members: &mut BTreeSet<RowRef>,
) -> Result<bool, StoreError> {
    let reader = Reader::new(connection, models);
    let mut grouped = BTreeMap::<ScopeRef, Vec<String>>::new();
    for (scope, row) in heads { grouped.entry(scope.clone()).or_default().push(row.to_string()); }
    let mut existing = BTreeMap::new();
    let mut first = BTreeMap::new();
    for (scope, ids) in grouped {
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        existing.insert(scope.clone(), reader.scope_members_among(scope.model.as_str(), scope.key.as_str(), &refs)?.into_iter().collect::<HashSet<_>>());
        first.insert(scope.clone(), reader.scope_first_key(scope.model.as_str(), scope.key.as_str())?);
    }
    let mut ops = Vec::new();
    for (scope, row) in heads {
        let Some(members) = existing.get_mut(scope) else { return Err(StoreError::Constraint("head scope is missing".into())); };
        if !members.insert(row.to_string()) { continue; }
        let order_key = crate::value::fractional_between(None, first.get(scope).and_then(Option::as_deref)).map_err(|error| StoreError::Constraint(error.to_string()))?;
        first.insert(scope.clone(), Some(order_key.clone()));
        ops.push(ScopeOp::Append { scope: scope.clone(), row: row.clone(), order_key });
    }
    let wrote = apply_scope_ops(connection, models, &ops, scopes, members)?;
    Ok(wrote)
}

#[derive(Default)]
pub(super) struct RowChanges {
    pub(super) changes: Vec<RowChange>,
    pub(super) renewed: Vec<RowRef>,
}

impl RowChanges {
    pub(super) fn push(&mut self, model: &ModelKey, id: &str, kind: RowChangeKind, fields: BTreeSet<String>) {
        if let Ok(row_id) = crate::identity::RowId::new(id) {
            self.changes.push(RowChange { row: RowRef::new(model.clone(), row_id), kind, fields });
        }
    }
}

pub(super) struct Destroyed {
    pub(super) model: ModelKey,
    pub(super) id: String,
    pub(super) reason: DestroyReason,
}

