use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{Map, Value};

use crate::commit::{
    CommitEnvelope, CommitResult, DestroyReason, EntityOp, OperationRecord, OperationTransition, EdgeValue, EdgeWrite, RowChange, RowChangeKind, ScopeOp, ScopeRef, WindowSave,
};
use crate::operation::OperationStatus;
use crate::commit::WriteOrigin;
use crate::ddl::{manifest_write, REVISION_SEQ_KEY};
use crate::identity::{Generation, ModelKey, RowRef};
use crate::operation::{delete_operation, OperationEvent, failure_cells, write_operation_error, read_rows, read_value, stored_row_id, write_rows, write_value};
use crate::syncstate::{operation_keys, recompute_sync_with, OperationFailure, SyncKey};

use super::file_ops::{apply_file_ops, release_files};
use super::rows::{column_value, fetch_stored, placeholders, Reader, StoredRow, CHUNK};
use super::search::{reindex, touches_search, unindex};
use super::stable;
use crate::nodes::{delete_trees, read_trees, texts, write_tree};
use super::shape::{delete_tables, insert_children, member_values, normalize, normalize_leaf, null_marker};
use crate::ddl::FieldPlan;
use super::windows::{upsert_window, WindowRecord};
use super::tombstones::{prune_tombstones, write_tombstone, TombstoneRetention};
use super::{pinned_epoch, Committed, Inner, ModelInfo, Store, StoreError};

#[derive(Debug, thiserror::Error)]
pub enum PlanCommitError<E> {
    #[error("planning failed")]
    Plan(E),
    #[error(transparent)]
    Store(StoreError),
}

impl<E> From<StoreError> for PlanCommitError<E> {
    fn from(error: StoreError) -> Self {
        PlanCommitError::Store(error)
    }
}

impl Store {
    pub fn commit(&self, envelope: CommitEnvelope) -> Result<CommitResult, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        inner.ensure_generation(envelope.generation)?;
        match self.write_planned(inner, Ok::<_, std::convert::Infallible>(Some(envelope))) {
            Ok(committed) => Ok(committed.publish()),
            Err(PlanCommitError::Store(error)) => Err(error),
            Err(PlanCommitError::Plan(never)) => match never {},
        }
    }

    fn plan_under_writer<E>(
        &self,
        generation: Generation,
        plan: impl FnOnce(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<Committed<'_>, PlanCommitError<E>> {
        let mut inner = self.lock();
        inner.ensure_healthy()?;
        inner.ensure_generation(generation)?;
        inner.counters.plans_under_writer += 1;
        let planned = plan(&Reader::new(&inner.connection, &inner.models));
        self.write_planned(inner, planned)
    }

    /// Plans on a read connection while the writer stays free. The writer only compares the revision the plan was read at and applies it. When another commit landed first, the writer is released and the plan runs again off the writer. Every repeat means another commit succeeded, so the loop has no attempt limit. Databases with `memory` models and in-memory databases plan under the writer. `plan` must only read and build from data it can build again.
    pub fn commit_prepared<E>(
        &self,
        generation: Generation,
        plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<CommitResult, PlanCommitError<E>> {
        self.commit_prepared_deferred(generation, plan).map(Committed::publish)
    }

    pub(crate) fn commit_prepared_deferred<E>(
        &self,
        generation: Generation,
        mut plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<Committed<'_>, PlanCommitError<E>> {
        if !self.reads_off_writer() {
            return self.plan_under_writer(generation, plan);
        }
        loop {
            let (revision, planned) = self.read(&[], |reader| Ok((reader.revision_seq()?, plan(reader))))?;
            #[cfg(test)]
            {
                let hook = self.before_write.lock().unwrap_or_else(|error| error.into_inner()).take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            let mut inner = self.lock();
            inner.ensure_healthy()?;
            inner.ensure_generation(generation)?;
            inner.counters.plans_off_writer += 1;
            if Reader::new(&inner.connection, &inner.models).revision_seq()? != revision {
                continue;
            }
            return self.write_planned(inner, planned);
        }
    }

    fn write_planned<'a, E>(&'a self, inner: std::sync::MutexGuard<'_, Inner>, planned: Result<Option<CommitEnvelope>, E>) -> Result<Committed<'a>, PlanCommitError<E>> {
        let now_ms = inner.now_ms();
        let envelope = planned.map_err(PlanCommitError::Plan)?;
        let Some(envelope) = envelope else {
            return Ok(Committed::unannounced(CommitResult::noop(uuid::Uuid::new_v4())));
        };
        envelope.validate(inner.generation).map_err(StoreError::Envelope)?;
        if envelope.is_empty() {
            return Ok(Committed::unannounced(CommitResult::noop(envelope.tx_id)));
        }
        match self.attempt_in_epoch(&inner, &envelope, now_ms) {
            Ok(result) => Ok(self.finish(inner, result)),
            Err(first) if first.is_apply_failure() => match self.attempt_in_epoch(&inner, &envelope, now_ms) {
                Ok(result) => Ok(self.finish(inner, result)),
                Err(second) => {
                    if second.is_apply_failure() {
                        *inner.poison.lock().unwrap_or_else(|error| error.into_inner()) = Some(second.to_string());
                    }
                    Err(second.into())
                }
            },
            Err(other) => Err(other.into()),
        }
    }

    /// The SQL commit of a write that belongs to a session epoch: the epoch check and the commit happen while the lifecycle holds its commit line, so a transition either comes before the check and refuses the write, or waits for the commit.
    fn attempt_in_epoch(&self, inner: &Inner, envelope: &CommitEnvelope, now_ms: i64) -> Result<(CommitResult, u32), StoreError> {
        let pinned = pinned_epoch();
        let _line = pinned.as_ref().map(|(life, _)| life.commit_line());
        if let Some((life, epoch)) = &pinned {
            if !life.epoch_holds(*epoch) { return Err(StoreError::Envelope(crate::commit::EnvelopeFault::StaleGeneration)); }
        }
        #[cfg(test)]
        if pinned.is_some() {
            let hook = self.after_epoch_check.lock().unwrap_or_else(|error| error.into_inner()).take();
            if let Some(hook) = hook { hook(); }
        }
        attempt(inner, envelope, now_ms)
    }

    fn finish<'a>(&'a self, mut inner: std::sync::MutexGuard<'_, Inner>, result: (CommitResult, u32)) -> Committed<'a> {
        inner.counters.unknown_operation_acks += result.1 as usize;
        self.commit_announcement(inner, result.0)
    }
}

fn attempt(inner: &Inner, envelope: &CommitEnvelope, now_ms: i64) -> Result<(CommitResult, u32), StoreError> {
    let transaction = inner.connection.unchecked_transaction()?;
    let outcome = apply_envelope(&transaction, &inner.models, envelope, now_ms)?;
    if outcome.is_noop() && envelope.window.is_none() && envelope.pending_locals.is_empty() {
        transaction.rollback()?;
    } else {
        transaction.commit()?;
    }
    let acks = outcome.unknown_operation_acks;
    Ok((outcome, acks))
}

fn diff_fields(old: &Map<String, Value>, new: &Map<String, Value>) -> BTreeSet<String> {
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

type HiddenRows = HashSet<(String, String)>;
type OpenDestroy = (i64, String);

fn hidden_rows(connection: &Connection, transitions: &[OperationTransition]) -> Result<HiddenRows, StoreError> {
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
    let mut statement = connection.prepare_cached("SELECT seq, action_key FROM op WHERE status IN ('pending', 'delivery_unknown') AND action_key LIKE '%:destroy'")?;
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
        let snapshot: Option<Value> = read_value(connection, seq, "prior")?;
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

fn visible_scope_ops(operations: &[ScopeOp], hidden: &HiddenRows) -> Vec<ScopeOp> {
    let is_hidden = |scope: &ScopeRef, row: &crate::identity::RowId| hidden.contains(&(scope.model.to_string(), row.to_string()));
    operations
        .iter()
        .filter_map(|operation| match operation {
            ScopeOp::Append { scope, row, .. } | ScopeOp::Move { scope, row, .. } if is_hidden(scope, row) => None,
            other => Some(other.clone()),
        })
        .collect()
}

fn take_pending(connection: &Connection, info: &ModelInfo, model: &ModelKey, id: &str, row: &mut Map<String, Value>) -> Result<(), StoreError> {
    let owner = texts(&[model.as_str(), id]);
    let pending = read_trees(connection, "local_pending", &["model", "row_id", "path"], "model = ?1 AND row_id = ?2", &owner)?;
    if pending.is_empty() {
        return Ok(());
    }
    for (key, value) in pending {
        if let Some(slot) = key.last().and_then(|path| match path { rusqlite::types::Value::Text(path) => info.spec.local_slot(path), _ => None }) {
            slot.write(row, value);
        }
    }
    delete_trees(connection, "local_pending", "model = ?1 AND row_id = ?2", &owner)?;
    Ok(())
}

fn without_nulls(info: &ModelInfo, row: &Map<String, Value>) -> Map<String, Value> {
    row.iter().filter(|(key, value)| !value.is_null() || info.spec.field(key).is_some_and(|field| field.nullable && field.default.is_some())).map(|(key, value)| (key.clone(), value.clone())).collect()
}

type Prefetched = HashMap<(String, String), Option<StoredRow>>;

fn prefetch_stored(connection: &Connection, models: &BTreeMap<String, ModelInfo>, operations: &[EntityOp], hidden: &HiddenRows) -> Result<Prefetched, StoreError> {
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

struct Landing<'a> {
    revision: i64,
    hidden: &'a HiddenRows,
    prefetched: &'a mut Prefetched,
    result: &'a mut RowChanges,
    moves: &'a mut Vec<ScopeOp>,
    heads: &'a mut Vec<(ScopeRef, crate::identity::RowId)>,
}

fn apply_upsert(
    connection: &Connection,
    info: &ModelInfo,
    model: &ModelKey,
    row: &Map<String, Value>,
    envelope: &CommitEnvelope,
    landing: Landing<'_>,
) -> Result<(), StoreError> {
    let Landing { revision, hidden, prefetched, result, moves, heads } = landing;
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
                take_pending(connection, info, model, &id, &mut next)?;
            }
        }
    }
    let row = &next;
    declared_membership(connection, info, model, &id, RowVersions { old: old_row, new: row }, envelope, MembershipLog { moves: &mut *moves, heads: &mut *heads })?;
    let (born_rev, mut field_rev, changed, kind) = match &existing {
        Some(stored) => {
            let changed = diff_fields(&stored.row, row);
            if changed.is_empty() {
                if let Ok(row_id) = crate::identity::RowId::new(&id) {
                    result.renewed.push(RowRef::new(model.clone(), row_id));
                }
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
    result.push(model, &id, kind, changed);
    Ok(())
}

fn canonicalize(info: &ModelInfo, row: &mut Map<String, Value>) {
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

struct RowRevisions<'a> {
    exists: bool,
    changed: &'a BTreeSet<String>,
    field_rev: &'a BTreeMap<String, i64>,
    revision: i64,
    born_rev: i64,
}

fn write_row(connection: &Connection, info: &ModelInfo, id: &str, row: &Map<String, Value>, revisions: RowRevisions<'_>) -> Result<(), StoreError> {
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
        let assignments: Vec<String> = names.iter().map(|name| format!("{} = ?", crate::ddl::sql_ident(name))).collect();
        values.push(id.to_string().into());
        connection.prepare_cached(&format!("UPDATE {} SET {} WHERE id = ?", info.table, assignments.join(", ")))?.execute(params_from_iter(values))?;
    } else {
        names.splice(0..0, ["id".to_string(), "born_rev".to_string()]);
        values.splice(0..0, [id.to_string().into(), born_rev.into()]);
        connection.prepare_cached(&format!("INSERT INTO {} ({}) VALUES ({})", info.table, names.iter().map(|name| crate::ddl::sql_ident(name)).collect::<Vec<_>>().join(", "), placeholders(names.len())))?.execute(params_from_iter(values))?;
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
    Ok(connection.execute(&format!("DELETE FROM {} WHERE id = ?1", info.table), params![id])?)
}

fn membership_moves(info: &ModelInfo, model: &ModelKey, id: &str, old: &Map<String, Value>, new: &Map<String, Value>, moves: &mut Vec<ScopeOp>, heads: &mut Vec<(ScopeRef, crate::identity::RowId)>) {
    let legacy = |row: &Map<String, Value>| info.spec.relations.iter().filter(|relation| relation.membership.is_empty() && !relation.joins_node && (relation.lists_every_row_of_its_params() || relation.takes_created_rows())).flat_map(|relation| relation.row_params(row).into_iter().filter_map(|params| info.spec.selection_key(&relation.name, &params).ok())).collect::<Vec<_>>();
    let before = legacy(old);
    let after = legacy(new);
    let Ok(row) = crate::identity::RowId::new(id) else { return };
    moves.extend(
        before.iter().filter(|key| !after.contains(key)).map(|key| ScopeOp::Detach { scope: ScopeRef::new(model.clone(), key.clone()), row: row.clone() }),
    );
    heads.extend(after.iter().filter(|key| !before.contains(key)).map(|key| (ScopeRef::new(model.clone(), key.clone()), row.clone())));
}

struct RowVersions<'a> {
    old: Option<&'a Map<String, Value>>,
    new: &'a Map<String, Value>,
}

struct MembershipLog<'a> {
    moves: &'a mut Vec<ScopeOp>,
    heads: &'a mut Vec<(ScopeRef, crate::identity::RowId)>,
}

fn declared_membership(connection: &Connection, info: &ModelInfo, model: &ModelKey, id: &str, versions: RowVersions<'_>, envelope: &CommitEnvelope, log: MembershipLog<'_>) -> Result<(), StoreError> {
    let RowVersions { old, new } = versions;
    let MembershipLog { moves, heads } = log;
    use crate::schema::{Membership, RemoteShape, PageDirection};
    let row = crate::identity::RowId::new(id).map_err(|error| StoreError::Constraint(error.to_string()))?;
    for relation in info.spec.relations.iter().filter(|relation| (!relation.membership.is_empty() || relation.has_threshold() || envelope.node_reread) && relation.constants_described() && (if envelope.node_reread { relation.joins_node } else { relation.joins_events && matches!(envelope.origin, WriteOrigin::Event | WriteOrigin::Response) })) {
        for rule in relation.membership.values() {
            let field = match rule { Membership::Contains(field) | Membership::Equals(field) => field };
            if !new.contains_key(field) { return Err(StoreError::Constraint(format!("missing membership field {field}"))); }
        }
        let keys = |values: &Map<String, Value>| if relation.has_threshold() {
            loaded_threshold_scopes(connection, info, relation, values)
        } else {
            relation.row_params(values).into_iter().map(|params| info.spec.selection_key(&relation.name, &params).map_err(|error| StoreError::Constraint(error.to_string()))).collect::<Result<BTreeSet<_>, _>>()
        };
        let before = old.map(keys).transpose()?.unwrap_or_default();
        let after = keys(new)?;
        moves.extend(before.difference(&after).map(|key| ScopeOp::Detach { scope: ScopeRef::new(model.clone(), key.clone()), row: row.clone() }));
        let direction = match relation.remote.as_ref().map(|remote| &remote.shape) { Some(RemoteShape::Connection { direction, .. }) => *direction, _ => PageDirection::Forward };
        for key in after {
            let window = super::windows::window_key(key.as_str(), direction);
            let loaded = connection.query_row("SELECT 1 FROM query_window WHERE key = ?1", [&window], |_| Ok(())).optional()?.is_some();
            if loaded { heads.push((ScopeRef::new(model.clone(), key), row.clone())); }
        }
    }
    Ok(())
}

fn loaded_threshold_scopes(connection: &Connection, info: &ModelInfo, relation: &crate::schema::RelationSpec, values: &Map<String, Value>) -> Result<BTreeSet<crate::identity::ListKey>, StoreError> {
    let constraint = |message: String| StoreError::Constraint(message);
    let wanted = relation.row_params(values).iter().map(|params| info.spec.partial_selection_params(&relation.name, params).map_err(|error| constraint(error.to_string()))).collect::<Result<Vec<_>, _>>()?;
    let mut keys = BTreeSet::new();
    if wanted.is_empty() { return Ok(keys); }
    let pattern = super::windows::like_prefix(&super::windows::family_prefix(info.spec.key.as_str(), &relation.name));
    let mut statement = connection.prepare_cached("SELECT key FROM query_window WHERE key LIKE ?1 ESCAPE '\\'")?;
    let windows = statement.query_map([pattern], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
    for window in windows {
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

fn join_heads(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    heads: &[(ScopeRef, crate::identity::RowId)],
    scopes: &mut BTreeSet<ScopeRef>,
    members: &mut BTreeSet<RowRef>,
) -> Result<bool, StoreError> {
    let mut wrote = false;
    for (scope, row) in heads {
        if Reader::new(connection, models).scope_has_member(scope.model.as_str(), scope.key.as_str(), row.as_str())? { continue; }
        let first = Reader::new(connection, models).scope_first_key(scope.model.as_str(), scope.key.as_str())?;
        let order_key = crate::value::fractional_between(None, first.as_deref()).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let append = ScopeOp::Append { scope: scope.clone(), row: row.clone(), order_key };
        wrote |= apply_scope_ops(connection, models, std::slice::from_ref(&append), scopes, members)?;
    }
    Ok(wrote)
}

#[derive(Default)]
struct RowChanges {
    changes: Vec<RowChange>,
    renewed: Vec<RowRef>,
}

impl RowChanges {
    fn push(&mut self, model: &ModelKey, id: &str, kind: RowChangeKind, fields: BTreeSet<String>) {
        if let Ok(row_id) = crate::identity::RowId::new(id) {
            self.changes.push(RowChange { row: RowRef::new(model.clone(), row_id), kind, fields });
        }
    }
}

struct Destroyed {
    model: ModelKey,
    id: String,
    reason: DestroyReason,
}

pub(crate) fn apply_envelope(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    envelope: &CommitEnvelope,
    now_ms: i64,
) -> Result<CommitResult, StoreError> {
    let reader = Reader::new(connection, models);
    let revision = reader.revision_seq()? + 1;
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
            DestroyReason::Replaced { by } => connection.execute(
                "UPDATE OR REPLACE scope_member SET row_id = ?1 WHERE model = ?2 AND row_id = ?3",
                params![by.as_str(), item.model.as_str(), item.id],
            )?,
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
                OperationTransition::RemoveUploadItem { seq, .. } | OperationTransition::SeparateUploads { seq, .. }
                | OperationTransition::Close { seq, .. } | OperationTransition::Fail { seq, .. }
                | OperationTransition::Start { seq } | OperationTransition::Postpone { seq, .. }
                | OperationTransition::Retry { seq } | OperationTransition::Remove { seq, .. } => Some(*seq),
            };
            if let Some(seq) = seq { progress_only.remove(&seq); }
        }
        for seq in result.operations.iter().filter(|seq| !progress_only.contains(seq)) {
            result.queue_operations.insert(*seq, crate::operation::read_queue_operation(connection, *seq)?);
        }
        if tombstoned {
            prune_tombstones(connection, now_ms, &TombstoneRetention::DEFAULT)?;
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
    if window.invalidated {
        connection.execute("UPDATE query_window SET invalidated = 1 WHERE key = ?1", [&window.key])?;
    }
    if let Some(marker) = &window.marker {
        connection.execute("DELETE FROM window_marker WHERE window_key = ?1", [&window.key])?;
        let mut insert = connection.prepare_cached("INSERT INTO window_marker (window_key, generation, sequence, field, value) VALUES (?1, ?2, ?3, ?4, ?5)")?;
        for (field, value) in &marker.values {
            insert.execute(params![window.key, generation.value() as i64, marker.sequence, field, value])?;
        }
    }
    if let Some(landed_at) = window.cleared_at {
        connection.execute("UPDATE query_window SET invalidated = 0, landed_at = ?1 WHERE key = ?2", params![landed_at, window.key])?;
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
        let Some(snapshot) = read_value(connection, *seq, "prior")? else { continue };
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
                    crate::operation::record_orphan_uploads(connection, seq, None)?;
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
            OperationTransition::RemoveUploadItem { seq, position, inputs } => {
                synced.extend(op_keys_of(connection, *seq)?);
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
            OperationTransition::SeparateUploads { seq, single_action, single_mutation } => {
                synced.extend(op_keys_of(connection, *seq)?);
                if connection.query_row("SELECT EXISTS (SELECT 1 FROM op WHERE seq = ?1 AND transport_started = 0 AND status = 'pending')", [seq], |row| row.get::<_, bool>(0))? {
                    result.operations.extend(super::upload::separate_items(connection, *seq, single_action, single_mutation, now_ms)?);
                    result.operations.insert(*seq);
                    wrote = true;
                }
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
                if *status == OperationStatus::RolledBack && connection.query_row("SELECT EXISTS (SELECT 1 FROM blob_upload b JOIN op o ON o.seq = b.consumer_seq WHERE o.seq = ?1 AND o.status = 'delivery_unknown')", [seq], |row| row.get::<_, bool>(0))? {
                    return Err(StoreError::Constraint("Blob owner has an unknown outcome".into()));
                }
                result.operations.extend(super::blob::release(connection, *seq, *status == OperationStatus::Committed)?);
                if *status == OperationStatus::RolledBack {
                    crate::operation::record_orphan_uploads(connection, *seq, None)?;
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
                    if connection.query_row("SELECT EXISTS (SELECT 1 FROM blob_upload b JOIN op o ON o.seq = b.consumer_seq WHERE o.seq = ?1 AND o.status = 'delivery_unknown')", [seq], |row| row.get::<_, bool>(0))? {
                        return Err(StoreError::Constraint("Blob owner has an unknown outcome".into()));
                    }
                    super::blob::release(connection, *seq, false)?;
                    crate::operation::record_orphan_uploads(connection, *seq, None)?;
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
    write_value(connection, seq, "prior", record.rollback.as_ref())?;
    if let Some(state) = &record.upload {
        crate::operation::write_upload(connection, seq, 0, state)?;
    }
    Ok(seq)
}

fn write_item_source(connection: &Connection, seq: i64, position: i64, item: &crate::commit::UploadAdmission) -> Result<(), StoreError> {
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
